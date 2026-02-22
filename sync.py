import os
import requests

# ── CONFIG ──────────────────────────────────────────────────────────────────
NOTION_TOKEN        = os.environ["NOTION_TOKEN"]
NOTION_BACKLOG_DB   = os.environ["NOTION_BACKLOG_DB_ID"]
NOTION_SPRINTS_DB   = os.environ["NOTION_SPRINTS_DB_ID"]
GITHUB_TOKEN        = os.environ["GH_PAT"]
GITHUB_REPO         = os.environ["GITHUB_REPOSITORY"]  # auto-set by GitHub Actions

NOTION_VERSION = "2022-06-28"

notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

github_headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}

# ── HELPERS ──────────────────────────────────────────────────────────────────

def get_github_issues():
    """Fetch all GitHub issues (open + closed)."""
    issues = []
    page = 1
    while True:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/issues",
            headers=github_headers,
            params={"state": "all", "per_page": 100, "page": page},
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        # exclude pull requests
        issues += [i for i in batch if "pull_request" not in i]
        page += 1
    return issues


def get_github_milestones():
    """Fetch all GitHub milestones (open + closed)."""
    milestones = []
    page = 1
    while True:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/milestones",
            headers=github_headers,
            params={"state": "all", "per_page": 100, "page": page},
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        milestones += batch
        page += 1
    return milestones


def query_notion_db(db_id):
    """Return all pages in a Notion DB as {github_url: page_id}."""
    results = {}
    cursor = None
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=notion_headers,
            json=payload,
        )
        r.raise_for_status()
        data = r.json()
        for page in data.get("results", []):
            url_prop = page["properties"].get("Github URL", {})
            url = url_prop.get("url") or ""
            if url:
                results[url] = page["id"]
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results


def map_issue_status(issue):
    """Map GitHub issue state + labels to Notion status."""
    if issue["state"] == "closed":
        return "Done"
    labels = [l["name"] for l in issue.get("labels", [])]
    if "in-review" in labels:
        return "In Review"
    if "in-progress" in labels:
        return "In Progress"
    if "backlog" in labels:
        return "Backlog"
    if "ice-box" in labels:
        return "Ice Box"
    return "New Issues"


def map_issue_type(issue):
    """Extract type label from issue labels."""
    labels = [l["name"] for l in issue.get("labels", [])]
    type_labels = [
        "architecture", "data-integration", "data-engineer",
        "data-analyst", "data-scientist", "bi", "security",
        "data-privacy", "data-science", "deployment",
        "documentation", "presentation", "all-roles",
    ]
    for l in labels:
        if l in type_labels:
            return l
    return None


def build_issue_properties(issue, sprint_url_map):
    """Build Notion properties dict for a GitHub issue."""
    body = issue.get("body") or ""

    # split description and acceptance criteria from body
    description = body
    acceptance = ""
    if "**Acceptance criteria**" in body:
        parts = body.split("**Acceptance criteria**")
        description = parts[0].strip()
        acceptance = parts[1].strip() if len(parts) > 1 else ""

    # find matching sprint page ID from milestone
    sprint_id = None
    if issue.get("milestone"):
        milestone_url = issue["milestone"]["html_url"]
        sprint_id = sprint_url_map.get(milestone_url)

    assignees = [a["login"] for a in issue.get("assignees", [])]

    props = {
        "Name": {"title": [{"text": {"content": issue["title"]}}]},
        "Github URL": {"url": issue["html_url"]},
        "Status": {"status": {"name": map_issue_status(issue)}},
        "Description": {"rich_text": [{"text": {"content": description[:2000]}}]},
        "Acceptance Criteria": {"rich_text": [{"text": {"content": acceptance[:2000]}}]},
        "Assigned To": {"rich_text": [{"text": {"content": ", ".join(assignees)}}]},
    }

    issue_type = map_issue_type(issue)
    if issue_type:
        props["Type"] = {"select": {"name": issue_type}}

    if sprint_id:
        props["Sprint"] = {"relation": [{"id": sprint_id}]}

    return props


def build_milestone_properties(milestone):
    """Build Notion properties dict for a GitHub milestone."""
    status = "Current" if milestone["state"] == "open" else "Completed"
    props = {
        "Name": {"title": [{"text": {"content": milestone["title"]}}]},
        "Github URL": {"url": milestone["html_url"]},
        "Status": {"status": {"name": status}},
        "Sprint Goal": {"rich_text": [{"text": {"content": milestone.get("description") or ""}}]},
    }
    if milestone.get("due_on"):
        due = milestone["due_on"][:10]
        props["Duration"] = {"date": {"start": due, "end": due}}
    return props


def create_notion_page(db_id, properties):
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers,
        json={"parent": {"database_id": db_id}, "properties": properties},
    )
    r.raise_for_status()
    return r.json()


def update_notion_page(page_id, properties):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers,
        json={"properties": properties},
    )
    r.raise_for_status()
    return r.json()


# ── MAIN SYNC ────────────────────────────────────────────────────────────────

def sync_milestones():
    print("── Syncing milestones → Sprints DB ──")
    milestones = get_github_milestones()
    existing = query_notion_db(NOTION_SPRINTS_DB)

    sprint_url_map = {}  # milestone html_url → notion page id

    for m in milestones:
        url = m["html_url"]
        props = build_milestone_properties(m)
        if url in existing:
            page_id = existing[url]
            update_notion_page(page_id, props)
            print(f"  updated: {m['title']}")
        else:
            page = create_notion_page(NOTION_SPRINTS_DB, props)
            page_id = page["id"]
            print(f"  created: {m['title']}")
        sprint_url_map[url] = page_id

    return sprint_url_map


def sync_issues(sprint_url_map):
    print("── Syncing issues → Backlog DB ──")
    issues = get_github_issues()
    existing = query_notion_db(NOTION_BACKLOG_DB)

    for issue in issues:
        url = issue["html_url"]
        props = build_issue_properties(issue, sprint_url_map)
        if url in existing:
            update_notion_page(existing[url], props)
            print(f"  updated: #{issue['number']} {issue['title']}")
        else:
            create_notion_page(NOTION_BACKLOG_DB, props)
            print(f"  created: #{issue['number']} {issue['title']}")


if __name__ == "__main__":
    sprint_map = sync_milestones()
    sync_issues(sprint_map)
    print("── Sync complete ──")
