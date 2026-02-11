# SQL-Databricks Bridge -- Frontend Design Specification

## 1. Overview

Internal tool for data engineers to trigger and monitor data sync operations
between SQL Server and Databricks Unity Catalog. Supports 9 LATAM countries.

- **Stack**: React + Vite + shadcn/ui + Tailwind CSS + MSAL.js
- **Deployment**: Static SPA on GitHub Pages
- **Auth**: Azure AD OAuth 2.0 (MSAL.js)
- **API**: FastAPI backend at `sql-databricks-bridge`

---

## 2. Component Hierarchy

```
App
├── AuthProvider (MSAL context)
│   ├── LoginPage
│   │   └── LoginCard
│   │       ├── Logo
│   │       └── AzureADLoginButton
│   │
│   └── AuthenticatedLayout
│       ├── TopBar
│       │   ├── AppTitle ("Bridge")
│       │   ├── NavTabs [Dashboard | History]
│       │   └── UserMenu (avatar, name, sign out)
│       │
│       ├── DashboardPage
│       │   ├── SyncTriggerCard
│       │   │   ├── CountrySelect (shadcn Select)
│       │   │   ├── TagInput (shadcn Input)
│       │   │   ├── QuerySelector (optional, shadcn multi-select)
│       │   │   └── TriggerButton (shadcn Button)
│       │   ├── ConfirmDialog (shadcn AlertDialog)
│       │   └── RecentJobsList
│       │       └── JobRow[] (country, tag, status badge, time)
│       │
│       ├── HistoryPage
│       │   ├── FilterBar
│       │   │   ├── CountryFilter (Select)
│       │   │   ├── StatusFilter (Select)
│       │   │   └── DateRangePicker
│       │   └── EventTable (shadcn Table)
│       │       └── EventRow[]
│       │           ├── CountryCell
│       │           ├── TagCell
│       │           ├── TriggeredByCell
│       │           ├── StatusBadge
│       │           ├── CreatedAtCell
│       │           └── ActionsCell (view detail)
│       │
│       └── EventDetailPage (route: /events/:jobId)
│           ├── JobSummaryCard
│           │   ├── StatusBadge (large)
│           │   ├── MetadataGrid (country, tag, triggered_by, timestamps)
│           │   └── ProgressBar (queries_completed / queries_total)
│           └── QueryResultsTable
│               └── QueryRow[]
│                   ├── QueryNameCell
│                   ├── StatusBadge
│                   ├── RowCountCell
│                   ├── DurationCell
│                   └── ErrorCell (collapsible)
```

---

## 3. Screen Wireframes

### 3.1 Login Screen

```
+----------------------------------------------------------+
|                                                          |
|                                                          |
|              +----------------------------+              |
|              |                            |              |
|              |     [Kantar Logo]          |              |
|              |                            |              |
|              |   SQL-Databricks Bridge    |              |
|              |                            |              |
|              |   Data sync operations     |              |
|              |   for LATAM countries      |              |
|              |                            |              |
|              | +------------------------+ |              |
|              | | Sign in with Microsoft | |              |
|              | +------------------------+ |              |
|              |                            |              |
|              +----------------------------+              |
|                                                          |
+----------------------------------------------------------+
```

**Components used**: `Card`, `Button` (with Microsoft icon)

**Behavior**:
- On click, MSAL.js redirects to Azure AD login
- On success, redirect to Dashboard
- On failure, show inline error message below button

---

### 3.2 Dashboard

```
+----------------------------------------------------------+
| Bridge               [Dashboard] [History]    J. Aldunate |
|                                                   [v]     |
+----------------------------------------------------------+
|                                                          |
|  +----------------------------------------------------+ |
|  | Trigger Data Sync                                   | |
|  |                                                     | |
|  | Country          Tag / Snapshot Label               | |
|  | +-------------+  +------------------------------+  | |
|  | | Bolivia   v |  | Calibracion Feb 2026         |  | |
|  | +-------------+  +------------------------------+  | |
|  |                                                     | |
|  | Queries (optional -- leave blank for all)           | |
|  | +------------------------------------------------+ | |
|  | | j_atoscompra_new, hato_cabecalho           [x] | | |
|  | +------------------------------------------------+ | |
|  |                                                     | |
|  |                         [ Trigger Sync ]            | |
|  +----------------------------------------------------+ |
|                                                          |
|  Recent Jobs                                             |
|  +----------------------------------------------------+ |
|  | Country  | Tag             | Status    | When       | |
|  |----------|-----------------|-----------|------------| |
|  | Bolivia  | Calibracion Feb | completed | 2 hrs ago  | |
|  | Chile    | Monthly Jan     | running   | 10 min ago | |
|  | Brazil   | Hotfix 3        | failed    | 1 day ago  | |
|  +----------------------------------------------------+ |
|                                                          |
+----------------------------------------------------------+
```

**Components used**: `Card`, `Select`, `Input`, `Button`, `Table`, `Badge`

**Country Select options** (9 total):
- Argentina, Bolivia, Brazil, CAM, Chile, Colombia, Ecuador, Mexico, Peru

**Tag Input**:
- Free-form text, max 64 chars
- Placeholder: "e.g., Calibracion Feb 2026"
- Required field

**Query Selector** (optional):
- Multi-select with search/filter
- Lists query names from backend (e.g., `j_atoscompra_new`, `hato_cabecalho`, `dt_mes`)
- Empty = sync all queries for the country
- Shows count badge: "(3 selected)"

**Trigger Button**:
- Primary variant, disabled until country + tag are filled
- On click, opens ConfirmDialog

**Recent Jobs**:
- Shows last 5 jobs sorted by creation time
- Clickable rows navigate to Event Detail

---

### 3.3 Confirm Dialog

```
+----------------------------------------------+
|  Confirm Sync Trigger                        |
|                                              |
|  You are about to trigger a data sync:       |
|                                              |
|  Country:  Bolivia                           |
|  Tag:      Calibracion Feb 2026              |
|  Queries:  All (39 queries)                  |
|                                              |
|  This will extract data from SQL Server      |
|  and write to Databricks Unity Catalog.      |
|                                              |
|            [ Cancel ]  [ Confirm & Start ]   |
+----------------------------------------------+
```

**Components used**: `AlertDialog`, `Button`

**Behavior**:
- "Cancel" closes dialog
- "Confirm & Start" sends POST /extract, closes dialog, shows toast, redirects to Event Detail

---

### 3.4 History Page

```
+----------------------------------------------------------+
| Bridge               [Dashboard] [History]    J. Aldunate |
+----------------------------------------------------------+
|                                                          |
|  +-------------+ +------------+ +--------------------+   |
|  | All       v | | All      v | | Last 30 days     v |  |
|  | (Country)   | | (Status)   | | (Date range)       |  |
|  +-------------+ +------------+ +--------------------+   |
|                                                          |
|  +----------------------------------------------------+ |
|  | Country | Tag           | By         | Status | At  | |
|  |---------|---------------|------------|--------|-----| |
|  | Bolivia | Calibr. Feb   | J.Aldunate |  done  | 2/9 | |
|  | Chile   | Monthly Jan   | M.Perez    |  run   | 2/8 | |
|  | Brazil  | Hotfix 3      | J.Aldunate | fail   | 2/7 | |
|  | Bolivia | Weekly W5     | System     |  done  | 2/5 | |
|  | Mexico  | Calibr. Jan   | J.Aldunate |  done  | 1/30| |
|  | ...     | ...           | ...        | ...    | ... | |
|  +----------------------------------------------------+ |
|                                                          |
|  Showing 1-20 of 142               [ < ]  1 2 3  [ > ]  |
|                                                          |
+----------------------------------------------------------+
```

**Components used**: `Select` (filters), `Table`, `Badge`, pagination

**Filters**:
- Country: dropdown with "All" + 9 countries
- Status: "All", "Pending", "Running", "Completed", "Failed", "Cancelled"
- Date range: preset options ("Last 7 days", "Last 30 days", "All time")

**Table columns**:
- Country (text)
- Tag (text, truncated at 30 chars with tooltip)
- Triggered By (user display name)
- Status (Badge with color)
- Created At (relative time or date)

**Pagination**: Server-side, 20 per page

---

### 3.5 Event Detail Page

```
+----------------------------------------------------------+
| Bridge               [Dashboard] [History]    J. Aldunate |
+----------------------------------------------------------+
|                                                          |
|  < Back to History                                       |
|                                                          |
|  +----------------------------------------------------+ |
|  |  [====== COMPLETED ======]                          | |
|  |                                                     | |
|  |  Country:      Bolivia                              | |
|  |  Tag:          Calibracion Feb 2026                 | |
|  |  Triggered by: J. Aldunate                          | |
|  |  Started:      2026-02-09 14:30:00 UTC              | |
|  |  Completed:    2026-02-09 14:45:23 UTC              | |
|  |  Duration:     15m 23s                              | |
|  |                                                     | |
|  |  Progress: [##########################----] 36/39   | |
|  +----------------------------------------------------+ |
|                                                          |
|  Query Results                                           |
|  +----------------------------------------------------+ |
|  | Query Name              | Status | Rows    | Time  | |
|  |-------------------------|--------|---------|-------| |
|  | j_atoscompra_new        |  done  | 1.2M    | 4m 2s | |
|  | hato_cabecalho          |  done  | 340K    | 1m 8s | |
|  | dt_mes                  |  done  | 1,200   | 0.3s  | |
|  | a_produtocoef..         |  fail  | 0       | 2.1s  | |
|  |   Error: Connection timeout after 30s       [copy] | |
|  | ...                     |  ...   |  ...    | ...   | |
|  +----------------------------------------------------+ |
|                                                          |
+----------------------------------------------------------+
```

**Components used**: `Card`, `Badge`, `Progress`, `Table`, `Collapsible`

**Job Summary Card**:
- Large status badge at top (color-coded)
- Metadata grid: country, tag, triggered_by, timestamps, duration
- Progress bar: `queries_completed / queries_total`

**Query Results Table**:
- Query Name: stem name of the SQL file
- Status: Badge (completed/failed/running/pending)
- Rows: formatted number (e.g., "1.2M", "340K")
- Duration: human-readable
- Error rows: expandable with error message and copy button

---

## 4. Color Palette and Typography

### Colors (shadcn/ui defaults, Zinc theme)

| Role             | Token                | Value     | Usage                          |
|------------------|----------------------|-----------|--------------------------------|
| Background       | `--background`       | white     | Page background                |
| Foreground       | `--foreground`       | zinc-950  | Primary text                   |
| Card             | `--card`             | white     | Card surfaces                  |
| Primary          | `--primary`          | zinc-900  | Buttons, active tabs           |
| Secondary        | `--secondary`        | zinc-100  | Secondary buttons, hover       |
| Muted            | `--muted`            | zinc-100  | Muted text, disabled           |
| Accent           | `--accent`           | zinc-100  | Hover states                   |
| Destructive      | `--destructive`      | red-500   | Error states, failed badges    |
| Border           | `--border`           | zinc-200  | Card borders, dividers         |
| Ring             | `--ring`             | zinc-950  | Focus rings                    |

### Status Badge Colors

| Status    | Variant        | Color        |
|-----------|----------------|--------------|
| Pending   | `outline`      | zinc (gray)  |
| Running   | `default`      | blue-500     |
| Completed | `default`      | green-500    |
| Failed    | `destructive`  | red-500      |
| Cancelled | `secondary`    | zinc-400     |

### Typography

| Element        | Font               | Size    | Weight     |
|----------------|--------------------|---------|------------|
| Page title     | Inter (system)     | 24px    | semibold   |
| Card title     | Inter              | 18px    | semibold   |
| Body text      | Inter              | 14px    | normal     |
| Table header   | Inter              | 13px    | medium     |
| Table cell     | Inter              | 14px    | normal     |
| Badge text     | Inter              | 12px    | medium     |
| Muted/caption  | Inter              | 12px    | normal     |
| Monospace      | JetBrains Mono     | 13px    | normal     |

Use `font-sans` (Inter via Tailwind) as default. Use monospace for
query names, error messages, and technical identifiers.

---

## 5. Interaction Flows

### 5.1 Login Flow

```
User opens app
  |
  v
[LoginPage] -- Is token cached in MSAL? --yes--> [Dashboard]
  |
  no
  |
  v
User clicks "Sign in with Microsoft"
  |
  v
MSAL.js redirect to Azure AD
  |
  v
Azure AD authenticates user
  |
  v
Redirect back to app with token
  |
  v
App stores token, fetches user profile
  |
  v
[Dashboard]
```

### 5.2 Trigger Sync Flow

```
[Dashboard]
  |
  v
User selects Country from dropdown
  |
  v
User types Tag/snapshot label
  |
  v
(Optional) User selects specific queries
  |
  v
User clicks "Trigger Sync"
  |
  v
[ConfirmDialog] shows summary
  |
  +-- Cancel --> [Dashboard] (no action)
  |
  +-- Confirm --> POST /extract
                    |
                    v
                 Show toast: "Sync job started"
                    |
                    v
                 Navigate to [EventDetailPage]
                    |
                    v
                 Poll GET /jobs/{id} every 5s
                    |
                    v
                 Update progress bar + query table
                    |
                    v
                 Job done? --> Show final status toast
```

### 5.3 Browse History Flow

```
[Dashboard] -- click "History" tab --> [HistoryPage]
  |
  v
Load events: GET /jobs?limit=20&offset=0
  |
  v
User applies filters (country, status, date)
  |
  v
Reload with filters: GET /jobs?country=bolivia&status=completed
  |
  v
User clicks a row --> [EventDetailPage]
```

### 5.4 Event Detail Flow

```
[HistoryPage] -- click row --> [EventDetailPage]
  |
  v
Load job: GET /jobs/{id}
  |
  v
Display summary card + query results table
  |
  v
If status == running:
  Poll every 5s, update progress + results
  |
  v
User clicks "Back to History" --> [HistoryPage]
```

---

## 6. Routing

| Path                | Page             | Description                   |
|---------------------|------------------|-------------------------------|
| `/`                 | LoginPage        | Azure AD login                |
| `/dashboard`        | DashboardPage    | Trigger sync + recent jobs    |
| `/history`          | HistoryPage      | Event history table           |
| `/events/:jobId`    | EventDetailPage  | Job detail + query progress   |

All routes except `/` require authentication. Unauthenticated users are
redirected to `/`.

---

## 7. Responsive Behavior

The app targets desktop use (data engineers at workstations). Minimum
supported width: 1024px. No mobile-specific layouts required.

- Cards stack vertically on narrower viewports
- Table columns hide gracefully (duration column hides first)
- Filters wrap to next line below 1280px

---

## 8. Accessibility Notes

- All form fields have visible labels (not just placeholders)
- Status badges use both color and text (not color alone)
- Focus management: dialog traps focus, returns focus on close
- Toast notifications include role="alert" for screen readers
- Table rows are keyboard-navigable

---

## 9. Error States

| Scenario                    | UI Treatment                                     |
|-----------------------------|--------------------------------------------------|
| Login failure               | Inline error below login button                  |
| API unreachable             | Full-page error with "Retry" button              |
| Sync trigger fails (400)    | Toast with error message                         |
| Sync trigger fails (500)    | Toast: "Server error, try again later"           |
| Job not found (404)         | "Job not found" message with link to History     |
| Empty history               | Empty state: "No sync jobs yet" with CTA         |
| Query failure in job        | Red row in query table, expandable error detail  |

---

## 10. Loading States

| Scenario               | UI Treatment                                      |
|------------------------|---------------------------------------------------|
| Initial page load      | Skeleton cards (shadcn Skeleton)                  |
| Submitting sync        | Button shows spinner, disabled                    |
| Loading history table  | Table skeleton rows (3-5 rows)                    |
| Polling job status     | Subtle spinner next to status badge               |
| Loading event detail   | Skeleton for summary card + table                 |
