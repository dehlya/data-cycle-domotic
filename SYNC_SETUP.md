# GitHub ↔ Notion Sync

Automatically syncs GitHub Issues → Notion Backlog DB and GitHub Milestones → Notion Sprints DB.

## How it works

- **Auto sync** — triggers on every issue or milestone create/edit/close/reopen
- **Manual sync** — trigger from GitHub Actions tab → "Sync GitHub → Notion" → Run workflow

## Setup

### 1. Copy files to your repo
```
sync.py
.github/workflows/notion-sync.yml
```

### 2. Add GitHub repo secrets
Go to your repo → Settings → Secrets and variables → Actions → New repository secret

| Secret name | Value |
|---|---|
| `NOTION_TOKEN` | Your Notion internal integration token |
| `NOTION_BACKLOG_DB_ID` | Your Notion Backlog db ID |
| `NOTION_SPRINTS_DB_ID` | Your Notion Sprint db ID |
| `GH_PAT` | Your GitHub Personal Access Token (`repo` + `workflow` scopes) |

### 3. Connect the Notion integration to your DBs
In Notion → open each DB → `...` → Connections → add your integration

## Field mapping

### Issues → Backlog DB
| GitHub | Notion |
|---|---|
| Title | Name |
| Body (top) | Description |
| Body (after "Acceptance criteria") | Acceptance Criteria |
| State + labels | Status |
| Labels | Type |
| Assignees | Assigned To |
| Milestone | Sprint (relation) |
| html_url | Github URL (dedup key) |

### Milestones → Sprints DB
| GitHub | Notion |
|---|---|
| Title | Name |
| Description | Sprint Goal |
| State | Status |
| Due date | Duration |
| html_url | Github URL (dedup key) |

## Status mapping

| GitHub state/label | Notion status |
|---|---|
| open | New Issues |
| label: backlog | Backlog |
| label: in-progress | In Progress |
| label: in-review | In Review |
| closed | Done |
| label: ice-box | Ice Box |

## Manual trigger from Notion
You can add a button in Notion that opens this URL directly:
`https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/notion-sync.yml`
