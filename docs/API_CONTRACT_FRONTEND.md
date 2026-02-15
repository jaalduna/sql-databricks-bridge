# API Contract: Frontend Auth & Sync Triggers

**Version**: 1.0.0-draft
**Date**: 2026-02-10
**Status**: Design (M1)
**Base URL**: `{BRIDGE_HOST}/api/v1`

---

## Table of Contents

1. [Auth Flow](#1-auth-flow)
2. [Endpoints](#2-endpoints)
   - [Auth](#21-auth)
   - [Trigger](#22-trigger)
   - [Events](#23-events)
   - [Metadata](#24-metadata)
3. [Schemas](#3-schemas)
4. [Error Format](#4-error-format)
5. [CORS Policy](#5-cors-policy)
6. [Auth Flow Diagram](#6-auth-flow-diagram)

---

## 1. Auth Flow

The frontend (React SPA on GitHub Pages) authenticates users via **Azure AD / MSAL.js**. The backend validates JWT tokens issued by Azure AD.

**Token flow:**

1. User signs in with MSAL.js (Azure AD popup/redirect).
2. Frontend receives an **ID token** (JWT) from Azure AD.
3. Frontend sends the JWT as `Authorization: Bearer <token>` on every API request.
4. Backend validates the JWT signature against Azure AD's JWKS endpoint.
5. Backend extracts `preferred_username` (email) from token claims.
6. Backend checks the email against an **authorized users allowlist** (YAML config file, same pattern as existing `config/permissions.yaml`).
7. If the user is authorized, the request proceeds. Otherwise, 403 Forbidden.

**Azure AD config required (backend env vars):**

| Variable | Description | Example |
|----------|-------------|---------|
| `AZURE_AD_TENANT_ID` | Azure AD tenant ID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `AZURE_AD_CLIENT_ID` | App Registration client ID (audience) | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `AZURE_AD_ISSUER` | Token issuer URL (optional, derived from tenant) | `https://login.microsoftonline.com/{tenant}/v2.0` |

---

## 2. Endpoints

### 2.1 Auth

#### `GET /auth/me`

Returns the current authenticated user's identity and permissions. Used by the frontend to check login status and display user info.

**Headers:**
```
Authorization: Bearer <azure_ad_jwt>
```

**Response `200 OK`:**
```json
{
  "email": "joaquin.aldunate@wp.numerator.com",
  "name": "Joaquin Aldunate",
  "roles": ["admin"],
  "countries": ["bolivia", "brazil", "chile", "colombia", "ecuador", "mexico", "peru", "cam", "argentina"]
}
```

The `countries` array indicates which countries this user is authorized to trigger syncs for. An empty array means no sync permissions (read-only). The special value `["*"]` means all countries.

**Response `401 Unauthorized`:**
```json
{
  "error": "unauthorized",
  "message": "Invalid or expired token"
}
```

**Response `403 Forbidden`:**
```json
{
  "error": "forbidden",
  "message": "User not in authorized allowlist"
}
```

---

### 2.2 Trigger

#### `POST /trigger`

Triggers a data extraction (SQL Server -> Databricks) for specific queries in a country. This replaces the current `POST /extract` for frontend usage -- it is a simplified, user-facing wrapper that resolves paths internally.

**Headers:**
```
Authorization: Bearer <azure_ad_jwt>
```

**Request Body:**
```json
{
  "country": "bolivia",
  "queries": ["j_atoscompra_new", "a_canal", "a_produto"]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `country` | `string` | yes | Country code. Must be in user's `countries` list. |
| `queries` | `string[]` | no | Specific query names to run. `null` or omitted = all queries for that country. |

**Response `202 Accepted`:**
```json
{
  "job_id": "job_2026-02-10T15-30-00_bolivia_abc123",
  "status": "pending",
  "country": "bolivia",
  "queries": ["j_atoscompra_new", "a_canal", "a_produto"],
  "queries_count": 3,
  "created_at": "2026-02-10T15:30:00Z",
  "triggered_by": "joaquin.aldunate@wp.numerator.com"
}
```

**Response `400 Bad Request`:**
```json
{
  "error": "validation_error",
  "message": "Unknown queries for country 'bolivia': ['nonexistent_query']"
}
```

**Response `403 Forbidden`:**
```json
{
  "error": "forbidden",
  "message": "User not authorized for country 'brazil'"
}
```

---

### 2.3 Events

#### `GET /events`

Lists extraction jobs visible to the current user, with filtering and pagination.

**Headers:**
```
Authorization: Bearer <azure_ad_jwt>
```

**Query Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `country` | `string` | (all) | Filter by country |
| `status` | `string` | (all) | Filter by status: `pending`, `running`, `completed`, `failed`, `cancelled` |
| `limit` | `int` | 50 | Max results (1-200) |
| `offset` | `int` | 0 | Pagination offset |

**Response `200 OK`:**
```json
{
  "items": [
    {
      "job_id": "job_2026-02-10T15-30-00_bolivia_abc123",
      "status": "running",
      "country": "bolivia",
      "queries_total": 3,
      "queries_completed": 1,
      "queries_failed": 0,
      "created_at": "2026-02-10T15:30:00Z",
      "started_at": "2026-02-10T15:30:01Z",
      "completed_at": null,
      "triggered_by": "joaquin.aldunate@wp.numerator.com",
      "error": null
    }
  ],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```

Admin users see all events. Non-admin users see only their own events.

---

#### `GET /events/{job_id}`

Get detailed status of a specific extraction job, including per-query progress.

**Headers:**
```
Authorization: Bearer <azure_ad_jwt>
```

**Response `200 OK`:**
```json
{
  "job_id": "job_2026-02-10T15-30-00_bolivia_abc123",
  "status": "running",
  "country": "bolivia",
  "queries_total": 3,
  "queries_completed": 1,
  "queries_failed": 0,
  "created_at": "2026-02-10T15:30:00Z",
  "started_at": "2026-02-10T15:30:01Z",
  "completed_at": null,
  "triggered_by": "joaquin.aldunate@wp.numerator.com",
  "error": null,
  "results": [
    {
      "query_name": "j_atoscompra_new",
      "status": "completed",
      "rows_extracted": 245892,
      "table_name": "kpi_prd_01.bolivia.j_atoscompra_new",
      "duration_seconds": 42.3,
      "error": null
    },
    {
      "query_name": "a_canal",
      "status": "running",
      "rows_extracted": 0,
      "table_name": null,
      "duration_seconds": 0.0,
      "error": null
    },
    {
      "query_name": "a_produto",
      "status": "pending",
      "rows_extracted": 0,
      "table_name": null,
      "duration_seconds": 0.0,
      "error": null
    }
  ]
}
```

**Response `404 Not Found`:**
```json
{
  "error": "not_found",
  "message": "Job not found: job_xxx"
}
```

---

### 2.4 Metadata

#### `GET /metadata/countries`

Returns the list of supported countries and their available queries. Public endpoint (no auth required, but respects CORS).

**Response `200 OK`:**
```json
{
  "countries": [
    {
      "code": "bolivia",
      "queries": ["a_accesocanal", "a_canal", "a_conteudoproduto", "j_atoscompra_new", "..."],
      "queries_count": 39
    },
    {
      "code": "brazil",
      "queries": ["a_accesocanal", "a_canal", "..."],
      "queries_count": 39
    }
  ]
}
```

This endpoint allows the frontend to populate the country dropdown and the query multi-select without hardcoding values.

---

## 3. Schemas

### TriggerRequest

```python
class TriggerRequest(BaseModel):
    country: str = Field(..., description="Country code (e.g. 'bolivia', 'brazil')")
    queries: list[str] | None = Field(
        default=None,
        description="Specific queries to run. null = all queries for the country.",
    )
```

### TriggerResponse

```python
class TriggerResponse(BaseModel):
    job_id: str
    status: Literal["pending"]
    country: str
    queries: list[str]
    queries_count: int
    created_at: datetime
    triggered_by: str
```

### EventListResponse

```python
class EventListResponse(BaseModel):
    items: list[EventSummary]
    total: int
    limit: int
    offset: int
```

### EventSummary

```python
class EventSummary(BaseModel):
    job_id: str
    status: JobStatus
    country: str
    queries_total: int
    queries_completed: int
    queries_failed: int
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    triggered_by: str
    error: str | None
```

### EventDetail

Extends `EventSummary` with:

```python
class EventDetail(EventSummary):
    results: list[QueryResult]
```

### UserInfo

```python
class UserInfo(BaseModel):
    email: str
    name: str
    roles: list[str]
    countries: list[str]
```

### CountryInfo

```python
class CountryInfo(BaseModel):
    code: str
    queries: list[str]
    queries_count: int

class CountriesResponse(BaseModel):
    countries: list[CountryInfo]
```

---

## 4. Error Format

All error responses use a consistent format:

```json
{
  "error": "error_code",
  "message": "Human-readable description"
}
```

**Standard error codes:**

| HTTP Status | `error` code | When |
|-------------|--------------|------|
| 400 | `validation_error` | Invalid request body or query params |
| 401 | `unauthorized` | Missing, invalid, or expired JWT |
| 403 | `forbidden` | Valid JWT but user not authorized |
| 404 | `not_found` | Resource does not exist |
| 409 | `conflict` | Duplicate job or state conflict |
| 422 | `unprocessable_entity` | Request understood but cannot be processed |
| 500 | `internal_error` | Unexpected server error |
| 503 | `service_unavailable` | Dependency down (SQL Server, Databricks) |

---

## 5. CORS Policy

**Production (GitHub Pages):**

```python
CORSMiddleware(
    allow_origins=[
        "https://kantar-org.github.io",
        "https://*.github.io",  # For dev forks
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
    expose_headers=["X-Request-Id"],
    max_age=3600,
)
```

**Development (local):**

When `environment == "development"`, allow all origins (`["*"]`) -- this already exists in `main.py`.

**Recommendation**: Add `CORS_ALLOWED_ORIGINS` as an env var (comma-separated) so it can be configured per deployment without code changes.

---

## 6. Auth Flow Diagram

```
Frontend (GitHub Pages)                    Backend (FastAPI)                    Azure AD
        |                                       |                                  |
        |--- 1. MSAL.js login popup ----------->|                                  |
        |                                       |                                  |
        |<----- 2. ID token (JWT) --------------|                                  |
        |                                       |                                  |
        |--- 3. GET /auth/me ------------------>|                                  |
        |    Authorization: Bearer <jwt>        |                                  |
        |                                       |--- 4. Fetch JWKS keys ---------->|
        |                                       |<--- 5. Public keys --------------|
        |                                       |                                  |
        |                                       |--- 6. Validate JWT signature     |
        |                                       |--- 7. Check exp, aud, iss        |
        |                                       |--- 8. Extract email from claims  |
        |                                       |--- 9. Check email in allowlist   |
        |                                       |                                  |
        |<-- 10. 200 { email, name, roles } ----|                                  |
        |                                       |                                  |
        |--- 11. POST /trigger { country, ... }->|                                 |
        |    Authorization: Bearer <jwt>         |                                 |
        |                                        |--- 12. Validate JWT (cached)    |
        |                                        |--- 13. Check country permission |
        |                                        |--- 14. Start extraction job     |
        |<-- 15. 202 { job_id, status } ---------|                                 |
        |                                        |                                 |
        |--- 16. GET /events/{job_id} ---------->|                                 |
        |<-- 17. 200 { status, results[] } ------|                                 |
```

**JWKS caching**: The backend should cache the Azure AD JWKS keys (typically for 24h) to avoid hitting the JWKS endpoint on every request. Use `PyJWT` with `PyJWKClient` which handles caching internally.

---

## Integration Notes

### Relationship to existing endpoints

The new endpoints coexist with the existing API. Here is how they map:

| New endpoint | Wraps existing | Difference |
|---|---|---|
| `POST /trigger` | `POST /extract` | Simplified body (no paths), resolves `queries_path` and `config_path` internally using `CountryAwareQueryLoader` |
| `GET /events` | `GET /jobs` | Adds `triggered_by` field, pagination wrapper, country filter |
| `GET /events/{id}` | `GET /jobs/{id}` | Same data, adds `triggered_by` |
| `GET /auth/me` | (new) | JWT validation + allowlist check |
| `GET /metadata/countries` | (new) | Reads from `queries/countries/` directory |

### Authorized users config

Add a `config/authorized_users.yaml` file (separate from existing `config/permissions.yaml` which handles table-level permissions):

```yaml
users:
  - email: "joaquin.aldunate@wp.numerator.com"
    name: "Joaquin Aldunate"
    roles: ["admin"]
    countries: ["*"]

  - email: "francisco.dev@wp.numerator.com"
    name: "Francisco"
    roles: ["operator"]
    countries: ["bolivia", "chile", "peru"]
```

Roles:
- `admin`: Can see all events, manage all countries.
- `operator`: Can trigger syncs for assigned countries, sees own events only.

### BridgeSettings additions

New fields to add to `BridgeSettings` in `core/config.py`:

```python
# Azure AD settings
azure_ad_tenant_id: str = Field(default="", description="Azure AD tenant ID")
azure_ad_client_id: str = Field(default="", description="Azure AD app client ID (audience)")

# CORS
cors_allowed_origins: str = Field(
    default="",
    description="Comma-separated allowed origins (empty = use default based on environment)",
)

# Authorized users
authorized_users_file: str = Field(default="config/authorized_users.yaml")
```
