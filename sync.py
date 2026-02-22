import os
import json
import requests

# ── CONFIG ───────────────────────────────────────────────────────────────────
NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
NOTION_BACKLOG_DB = os.environ["NOTION_BACKLOG_DB_ID"]
NOTION_SPRINTS_DB = os.environ["NOTION_SPRINTS_DB_ID"]
GITHUB_TOKEN      = os.environ["GH_PAT"]
GITHUB_REPO       = os.environ["GITHUB_REPOSITORY"]

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

# ── SCHEMA ───────────────────────────────────────────────────────────────────

def get_db_schema(db_id):
    r = requests.get(
        f"https://api.notion.com/v1/databases/{db_id}",
        headers=notion_headers,
    )
    r.raise_for_status()
    schema = {}
    for name, prop in r.json().get("properties", {}).items():
        entry = {"type": prop["type"]}
        if prop["type"] == "status":
            entry["options"] = [o["name"] for o in prop.get("status", {}).get("options", [])]
        elif prop["type"] == "select":
            entry["options"] = [o["name"] for o in prop.get("select", {}).get("options", [])]
        schema[name] = entry
    return schema


def safe_status(value, schema_entry):
    options = schema_entry.get("options", [])
    if value in options:
        return value
    for o in options:
        if o.lower() == value.lower():
            return o
    print(f"  WARN: status '{value}' not in options {options}, skipping field")
    return None

# ── GITHUB ───────────────────────────────────────────────────────────────────

def get_github_issues():
    issues, page = [], 1
    while True:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/issues",
            headers=github_headers,
            params={"state": "all", "per_page": 100, "page": page},
        )
        r.raise_for_status()
        batch = [i for i in r.json() if "pull_request" not in i]
        if not batch:
            break
        issues += batch
        page += 1
    return issues


def get_github_milestones():
    milestones, page = [], 1
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

# ── NOTION QUERY ─────────────────────────────────────────────────────────────

def query_notion_db(db_id):
    results, cursor = {}, None
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
            url = (page["properties"].get("Github URL") or {}).get("url") or ""
            if url:
                results[url] = page["id"]
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results

# ── PROPERTY BUILDERS ────────────────────────────────────────────────────────

def txt(value, limit=2000):
    return {"rich_text": [{"text": {"content": (value or "")[:limit]}}]}


def map_issue_status(issue):
    if issue["state"] == "closed":
        return "Done"
    labels = [l["name"] for l in issue.get("labels", [])]
    if "in-review" in labels:   return "In Review"
    if "in-progress" in labels: return "In Progress"
    if "backlog" in labels:     return "Backlog"
    if "ice-box" in labels:     return "Ice Box"
    return "New Issues"


def map_issue_type(issue):
    known = {
        "architecture", "data-integration", "data-engineer", "data-analyst",
        "data-scientist", "bi", "security", "data-privacy", "data-science",
        "deployment", "documentation", "presentation", "all-roles",
    }
    for l in issue.get("labels", []):
        if l["name"] in known:
            return l["name"]
    return None


def build_issue_props(issue, sprint_url_map, schema):
    body = issue.get("body") or ""
    description, acceptance = body, ""
    if "**Acceptance criteria**" in body:
        parts = body.split("**Acceptance criteria**")
        description = parts[0].strip()
        acceptance  = parts[1].strip() if len(parts) > 1 else ""

    props = {
        "Name":       {"title": [{"text": {"content": issue["title"]}}]},
        "Github URL": {"url": issue["html_url"]},
    }

    if schema.get("Status", {}).get("type") == "status":
        val = safe_status(map_issue_status(issue), schema["Status"])
        if val:
            props["Status"] = {"status": {"name": val}}

    if schema.get("Description", {}).get("type") == "rich_text":
        props["Description"] = txt(description)

    if schema.get("Acceptance Criteria", {}).get("type") == "rich_text":
        props["Acceptance Criteria"] = txt(acceptance)

    if schema.get("Assigned To", {}).get("type") == "rich_text":
        assignees = ", ".join(a["login"] for a in issue.get("assignees", []))
        props["Assigned To"] = txt(assignees)

    t = map_issue_type(issue)
    if t and schema.get("Type", {}).get("type") == "select":
        props["Type"] = {"select": {"name": t}}

    if issue.get("milestone") and schema.get("Sprint", {}).get("type") == "relation":
        sid = sprint_url_map.get(issue["milestone"]["html_url"])
        if sid:
            props["Sprint"] = {"relation": [{"id": sid}]}

    return props


def build_milestone_props(milestone, schema):
    props = {
        "Name":       {"title": [{"text": {"content": milestone["title"]}}]},
        "Github URL": {"url": milestone["html_url"]},
    }

    raw_status = "Current" if milestone["state"] == "open" else "Completed"
    s_schema = schema.get("Status", {})
    if s_schema.get("type") == "status":
        val = safe_status(raw_status, s_schema)
        if val:
            props["Status"] = {"status": {"name": val}}
    elif s_schema.get("type") == "select":
        val = safe_status(raw_status, s_schema)
        if val:
            props["Status"] = {"select": {"name": val}}

    goal = milestone.get("description") or ""
    if goal and schema.get("Sprint Goal", {}).get("type") == "rich_text":
        props["Sprint Goal"] = txt(goal)

    if milestone.get("due_on") and schema.get("Duration", {}).get("type") == "date":
        due = milestone["due_on"][:10]
        props["Duration"] = {"date": {"start": due, "end": due}}

    return props

# ── NOTION WRITE ─────────────────────────────────────────────────────────────

def create_page(db_id, properties):
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers,
        json={"parent": {"database_id": db_id}, "properties": properties},
    )
    if not r.ok:
        print(f"  ERROR create: {r.status_code} — {r.text}")
        r.raise_for_status()
    return r.json()


def update_page(page_id, properties):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers,
        json={"properties": properties},
    )
    if not r.ok:
        print(f"  ERROR update: {r.status_code} — {r.text}")
        r.raise_for_status()
    return r.json()

# ── SUMMARY ──────────────────────────────────────────────────────────────────

def write_summary(skipped_milestones, skipped_issues):
    """Write a GitHub Actions job summary and exit 1 if anything was skipped."""
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    lines = []

    if not skipped_milestones and not skipped_issues:
        lines.append("## ✅ Sync complete\n")
        lines.append("All milestones and issues synced successfully.")
    else:
        lines.append("## ⚠️ Sync completed with errors\n")
        lines.append(f"> {len(skipped_milestones)} milestone(s) and {len(skipped_issues)} issue(s) were skipped.\n")

        if skipped_milestones:
            lines.append(f"### Skipped milestones ({len(skipped_milestones)})\n")
            lines.append("| Milestone | Error |")
            lines.append("|---|---|")
            for title, error in skipped_milestones:
                lines.append(f"| {title} | `{error}` |")
            lines.append("")

        if skipped_issues:
            lines.append(f"### Skipped issues ({len(skipped_issues)})\n")
            lines.append("| Issue | Error |")
            lines.append("|---|---|")
            for title, error in skipped_issues:
                lines.append(f"| {title} | `{error}` |")

    if summary_file:
        with open(summary_file, "w") as f:
            f.write("\n".join(lines))

    # always print to console too
    print("\n".join(lines))

    if skipped_milestones or skipped_issues:
        raise SystemExit(1)  # marks the Action run as failed so you get notified

# ── SYNC ─────────────────────────────────────────────────────────────────────

def sync_milestones():
    print("── Syncing milestones → Sprints DB ──")
    schema = get_db_schema(NOTION_SPRINTS_DB)
    print(f"  Schema: { {k: v['type'] for k, v in schema.items()} }")

    milestones = get_github_milestones()
    existing   = query_notion_db(NOTION_SPRINTS_DB)
    sprint_map = {}
    skipped    = []

    for m in milestones:
        url   = m["html_url"]
        props = build_milestone_props(m, schema)
        try:
            if url in existing:
                page_id = existing[url]
                update_page(page_id, props)
                print(f"  updated: {m['title']}")
            else:
                page    = create_page(NOTION_SPRINTS_DB, props)
                page_id = page["id"]
                print(f"  created: {m['title']}")
            sprint_map[url] = page_id
        except Exception as e:
            print(f"  SKIPPED {m['title']}: {e}")
            skipped.append((m["title"], str(e)))

    return sprint_map, skipped


def sync_issues(sprint_map):
    print("── Syncing issues → Backlog DB ──")
    schema   = get_db_schema(NOTION_BACKLOG_DB)
    print(f"  Schema: { {k: v['type'] for k, v in schema.items()} }")

    issues   = get_github_issues()
    existing = query_notion_db(NOTION_BACKLOG_DB)
    skipped  = []

    for issue in issues:
        url   = issue["html_url"]
        props = build_issue_props(issue, sprint_map, schema)
        try:
            if url in existing:
                update_page(existing[url], props)
                print(f"  updated: #{issue['number']} {issue['title']}")
            else:
                create_page(NOTION_BACKLOG_DB, props)
                print(f"  created: #{issue['number']} {issue['title']}")
        except Exception as e:
            print(f"  SKIPPED #{issue['number']} {issue['title']}: {e}")
            skipped.append((f"#{issue['number']} {issue['title']}", str(e)))

    return skipped


if __name__ == "__main__":
    sprint_map, skipped_milestones = sync_milestones()
    skipped_issues = sync_issues(sprint_map)
    write_summary(skipped_milestones, skipped_issues)
    print("── Sync complete ──")