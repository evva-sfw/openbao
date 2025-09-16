# Azure Table Storage Backend for OpenBao

## Overview

This plugin provides a **physical storage backend** for [OpenBao](https://github.com/openbao/openbao), using **Azure Table Storage** as the persistence layer. It implements the `physical.Backend` interface and supports all required operations:

- Store secrets (`Put`)
- Retrieve secrets (`Get`)
- Delete secrets (`Delete`)
- List keys with optional prefix filtering (`List`)
- Paginated listing (`ListPage`)

### Key Features
- **Integration with Azure Table Storage** using the official [`aztables`](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/data/aztables) SDK.
- **Multiple authentication modes**:
  - Shared Key (auto-creates table on startup)
  - Azure AD Client Credentials (service principal)
  - Table-scoped SAS URL
- **Automatic table creation** when using Shared Key authentication.
- **Exponential backoff retries** for service client and table operations.
- **Prefix-based hierarchical listing** and pagination support.
- **Graceful handling of missing keys** (404 returns `nil` instead of error).

---

## Architecture and Data Model

- **PartitionKey:**  
  - `"secrets"` for root keys (no namespace).  
  - `"secrets-<namespaceID>"` for namespaced keys.  
  This ensures keys from different namespaces are isolated in Azure Table partitions.

- **RowKey:**  
  - The SHA-256 hash of the **full original key**, encoded as a 64-character hex string.  
  - This shortens long OpenBao keys and avoids forbidden characters.

- **OrigKey:**  
  - The full original OpenBao key (path), stored alongside the entity.  
  - Used for prefix filtering in `List`/`ListPage` and for debugging.

- **Entity:** Stored as JSON. Main fields are:
  - `Value` → raw secret bytes (stored as base64 in Azure)
  - `OrigKey` → original full key

Example stored entity:
```json
{
  "PartitionKey": "secrets-6d44e0a5-9140-1e9d-26a9-774cc34e3308",
  "RowKey": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "Value": "base64_encoded_secret_data",
  "OrigKey": "namespaces/6d44e0a5-9140-1e9d-26a9-774cc34e3308/logical/foo/bar"
}
```

## Configuration

This backend supports three ways to authenticate to Azure Table Storage:

1) **Shared Key**
   - `account_name`, `account_key`, `service_url`, `table_name` (all required)
   - The backend creates the table on startup (idempotent).
   - Example `service_url`: `https://<account>.table.core.windows.net`

2) **Azure AD (client credentials)**
   - `tenant_id`, `client_id`, `client_secret`, `table_url` (all required)
   - The backend **does not** create the table in this mode; ensure the table already exists assuming the principal has `Table Service Contributor` rights (or equivalent).
   - Example `table_url`: `https://<account>.table.core.windows.net/tableName`

3) **SAS URL**
   - `sas_url` (required)
   - **Recommended:** a *table-scoped* SAS URL like `https://<account>.table.core.windows.net/<table>?<sas>`.
   - The backend **does not** create the table in this mode; ensure the table already exists and the SAS has the required permissions (at least `raud` for read/add/update/delete).

The backend requires the following configuration parameters:
| Key                     | Description                                                                                     | Required (per auth method) | Default |
| ----------------------- | ----------------------------------------------------------------------------------------------- | -------------------------- | ------- |
| `account_name`          | Azure Storage account name                                                                      | Shared Key                 | -       |
| `account_key`           | Azure Storage account key                                                                       | Shared Key                 | -       |
| `table_name`            | Table to store secrets (auto-created in Shared Key mode only)                                   | Shared Key                 | -       |
| `service_url`           | Service endpoint, e.g. `https://<acct>.table.core.windows.net`                                  | Shared Key                 | -       |
| `tenant_id`             | Azure Active Directory tenant GUID for authentication                                           | Azure AD                   | -       |
| `client_id`             | Azure AD application (service principal) client ID                                              | Azure AD                   | -       |
| `client_secret`         | Azure AD client secret                                                                          | Azure AD                   | -       |
| `table_url`             | Table endpoint e.g., `https://<account>.table.core.windows.net`                                 | Azure AD                   | -       |
| `sas_url`               | Full SAS URL (must be table-scoped), e.g. `https://<acct>.table.core.windows.net/<table>?<sas>` | SAS URL                    | -       |
| `max_connect_retries`   | Max retries for creating the service client & table (startup)                                   | Optional                   | `1`     |
| `max_operation_retries` | Max retries for runtime operations (Put/Delete/List)                                            | Optional                   | `1`     |

Example OpenBao Configuration (HCL):
# Shared Key
```hcl
storage "azuretable" {
  account_name          = "..."
  account_key           = "..."
  service_url           = "https://<account>.table.core.windows.net"
  table_name            = "OpenBaoSecrets"
  max_connect_retries   = "5"
  max_operation_retries = "3"
}
```

# Azure AD (client credentials)
```hcl
storage "azuretable" {
  tenant_id             = "<tenant-guid>"
  client_id             = "<app-guid>"
  client_secret         = "<secret>"
  table_url             = "https://<account>.table.core.windows.net/OpenBaoSecrets"
  max_operation_retries = "3"
}
```

# SAS URL (table-scoped)
```hcl
storage "azuretable" {
  sas_url               = "https://<account>.table.core.windows.net/OpenBaoSecrets?<sas>"
  max_operation_retries = "3"
}
```

## Usage

### Backend Initialization
```go
logger := log.New(&log.LoggerOptions{Name: "azuretable"})
backend, err := azuretable.NewAzureTableBackend(conf, logger)
if err != nil {
    panic(err)
}
```

### Basic Operations
#### Put (Create/Update secret)
```go
err := backend.Put(ctx, &physical.Entry{
    Key: "namespaces/6d44e0a5-9140-1e9d-26a9-774cc34e3308/logical/foo/bar",
    Value: []byte("mysecret"),
})
```

#### Get (Retrieve secret)
```go
entry, err := backend.Get(ctx, "namespaces/6d44e0a5-9140-1e9d-26a9-774cc34e3308/logical/foo/bar")
// RowKey is hashed, so the plugin recomputes it automatically.
```

#### Delete (Remove secret)
```go
err := backend.Delete(ctx, "foo/bar")
```

#### List (List keys with optional prefix)
```go
keys, _ := backend.List(ctx, "namespaces/6d44e0a5-9140-1e9d-26a9-774cc34e3308/logical/")
fmt.Println(keys) // ["foo/"]
```

#### ListPage (Paginated listing)
```go
keys, err := backend.ListPage(ctx, "foo/", "afterKey", 10)
```

## Retries and Backoff

- Uses **cenkalti/backoff** for retries.
- **max_connect_retries**: Controls retries during startup.
- **max_operation_retries**: Controls retries for operations.
- Retries transient 5xx errors; permanent 4xx errors are returned immediately.

## Key Encoding

- The **RowKey** is `sha256(original_key)` encoded as a hex string.  
  This avoids Azure length/character restrictions and keeps keys short.

- The **OrigKey** field always contains the full original path, so listing and prefix matching work as expected.

- `safeRowKey` / `revertSafeRowKey` are still used internally for partition names (e.g., `secrets-<nsID>`), but not for RowKeys.

## Logging

- Uses HashiCorp’s hclog.
- Logs table creation events and retries.
- Prints success/failure outcomes.

## Integration tests (require a running Table service)

The integration tests need a live Azure Table Storage endpoint (either **Azurite** or a real Azure account).

Tests cover both root keys and namespaced keys.  
- Root keys use `PartitionKey = "secrets"`.  
- Namespaced keys use `PartitionKey = "secrets-<nsID>"`.

Both `List` and `ListPage` validate prefix filtering inside namespaces.

### Start Azurite (recommended for local dev)

**Docker (all services, including Table):**
```bash
docker run --rm -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite \
  azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0
```

### Node (if installed globally):
```bash
# If not installed: npm i -g azurite
azurite --tableHost 0.0.0.0 --tablePort 10002
```

### Set env vars (Azurite defaults)
```bash
export AZ_ACCOUNT_NAME=devstoreaccount1
export AZ_ACCOUNT_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
export AZ_SERVICE_URL='http://127.0.0.1:10002/devstoreaccount1'
```

### Run the tests
From the plugin directory:

```bash
cd plugins/azuretable
go test -v -run Integration
```

Or from repo root:
```bash
AZ_ACCOUNT_NAME=devstoreaccount1 \
AZ_ACCOUNT_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==' \
AZ_SERVICE_URL='http://127.0.0.1:10002/devstoreaccount1' \
go test -v ./plugins/azuretable -run Integration
```
