package azuretable

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/cenkalti/backoff/v4"
	log "github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/physical"
)

// AzureTableBackend implements the OpenBao physical.Backend interface
// using Azure Table Storage as the underlying storage.
type AzureTableBackend struct {
	client           *aztables.Client // Azure Table client used for all operations
	tableName        string           // The table where secrets are stored
	logger           log.Logger       // Logger to provide info/debug messages
	operationRetries int              // Number of retries for operations
}

// rawEntity represents the structure stored in Azure Table Storage.
// PartitionKey and RowKey are required by Azure Table Storage.
type rawEntity struct {
	PartitionKey string // Logical partition, here we use "secrets"
	RowKey       string // Unique key per entity, mapped from OpenBao key
	Value        []byte // Actual secret data stored as bytes
	OrigKey      string // Stores the original key used for listing
}

/*
NewAzureTableBackend initializes the Azure Table backend with the provided
configuration and returns a Backend interface instance.

The configuration map `conf` must include:
  - account_name: Azure Storage account name
  - account_key: Azure Storage account key
  - table_name: Table to use for storing secrets
  - service_url: Full URL of the Azure Table service endpoint
  - max_connect_retries: Maximum number of retry attempts for both creating the service client and creating the table on startup

The function will attempt to create the service client and the table,
retrying transient failures up to `max_connect_retries` times with
exponential backoff. If the table already exists, it is not treated as an error.
*/
func NewAzureTableBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	accountName := conf["account_name"]
	accountKey := conf["account_key"]
	tableName := conf["table_name"]
	serviceURL := conf["service_url"]

	// Set maximum retries for DB connection liveness check on startup.
	maxRetriesStr, ok := conf["max_connect_retries"]
	var err error
	var maxRetriesInt int
	if ok {
		maxRetriesInt, err = strconv.Atoi(maxRetriesStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_connect_retries parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_connect_retries set", "max_connect_retries", maxRetriesInt)
		}
	} else {
		maxRetriesInt = 1
	}

	// Set maximum retries for DB connection liveness check on startup.
	maxOpRetriesStr, ok := conf["max_operation_retries"]
	var maxOpRetriesInt int
	if ok {
		maxOpRetriesInt, err = strconv.Atoi(maxOpRetriesStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_operation_retries parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_operation_retries set", "max_operation_retries", maxOpRetriesInt)
		}
	} else {
		maxOpRetriesInt = 1
	}

	service, err := createServiceClientWithRetry(accountName, accountKey, serviceURL, maxRetriesInt)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Table client: %w", err)
	}

	err = createTableWithRetry(context.Background(), service, tableName, maxRetriesInt, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create table after retries: %w", err)
	}

	// Create a client scoped to the specific table
	tableClient := service.NewClient(tableName)

	return &AzureTableBackend{
		client:           tableClient,
		tableName:        tableName,
		logger:           logger,
		operationRetries: maxOpRetriesInt,
	}, nil
}

// createServiceClientWithRetry attempts to create an Azure Table Service client using the
// provided account name, account key, and service URL. If the creation fails, it retries
// the operation using an exponential backoff strategy, up to maxRetries attempts.
//
// The maximum number of retries is passed as maxRetries, which can be configured
// via `max_connect_retries` in the configuration.
//
// Returns the created ServiceClient if successful, or an error if all retry attempts fail.
func createServiceClientWithRetry(accountName, accountKey, serviceURL string, maxRetries int) (*aztables.ServiceClient, error) {
	var service *aztables.ServiceClient
	var err error

	// Configure exponential backoff
	expBackoff := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(maxRetries))

	retryErr := backoff.Retry(func() error {
		cred, credErr := aztables.NewSharedKeyCredential(accountName, accountKey)
		if credErr != nil {
			err = fmt.Errorf("failed to create credentials: %w", credErr)
			return err
		}

		service, err = aztables.NewServiceClientWithSharedKey(serviceURL, cred, nil)
		if err != nil {
			return fmt.Errorf("failed to create Azure Table service client: %w", err)
		}

		return nil
	}, expBackoff)

	if retryErr != nil {
		return nil, fmt.Errorf("failed to create service client after %d retries: %w", maxRetries, retryErr)
	}

	return service, nil
}

// createTableWithRetry attempts to create the given table using the provided service client.
// Retries are performed on transient failures using exponential backoff up to maxRetries attempts.
// If the table already exists, it is not treated as an error.
func createTableWithRetry(ctx context.Context, service *aztables.ServiceClient, tableName string, maxRetries int, logger log.Logger) error {
	expBackoff := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(maxRetries))

	return backoff.Retry(func() error {
		_, err := service.CreateTable(ctx, tableName, nil)
		if err != nil {
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) && aztables.TableErrorCode(respErr.ErrorCode) == aztables.TableAlreadyExists {
				// Not an error, just log and stop retrying
				logger.Info("Table already exists, continuing...", "table", tableName)
				return nil
			}
			logger.Warn("Failed to create table, retrying...", "table", tableName, "error", err)
			return err // will trigger retry
		}

		logger.Info("Table created successfully", "table", tableName)
		return nil
	}, expBackoff)
}

// Get retrieves a secret by key from Azure Table Storage.
// Implements the physical.Backend interface Get method.
func (b *AzureTableBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	partition := partitionForKey(key)
	rkHash, _ := rowKeyHashForKey(key)

	// Use a fixed PartitionKey "secrets" for all OpenBao secrets
	resp, err := b.client.GetEntity(ctx, partition, rkHash, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			// 404 Not Found is expected if key doesn't exist, return nil to signal that
			return nil, nil
		}

		// Any other error is a real failure.
		return nil, err
	}

	// Try to unmarshal entity value into a map[string]interface{}
	var properties map[string]any
	err = json.Unmarshal(resp.Value, &properties)
	if err != nil {
		// Could be raw bytes or opaque value, just return as-is
		fmt.Println("failed to unmarshal entity properties, returning raw value for key", key)
		return &physical.Entry{
			Key:   key,
			Value: resp.Value,
		}, nil
	}

	val, ok := properties["Value"]
	if !ok {
		return nil, fmt.Errorf("key exists but Value field missing")
	}

	// Decode depending on type: either []byte or base64 string
	switch v := val.(type) {
	case []byte:
		return &physical.Entry{Key: key, Value: v}, nil
	case string:
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, err
		}
		return &physical.Entry{Key: key, Value: decoded}, nil
	default:
		return nil, fmt.Errorf("unsupported Value type: %T", val)
	}
}

// Put creates or updates (upserts) a secret in Azure Table Storage.
// Implements the physical.Backend interface PUT method.
func (b *AzureTableBackend) Put(ctx context.Context, entry *physical.Entry) error {
	partition := partitionForKey(entry.Key)
	rkHash, _ := rowKeyHashForKey(entry.Key)

	// Store OrigKey so we can do prefix filters on it in List/ListPage.
	ent := rawEntity{
		PartitionKey: partition,
		RowKey:       rkHash,
		Value:        entry.Value, // Store the raw secret bytes
		OrigKey:      entry.Key,   // full original key
	}

	// Marshal the entity into to JSON
	entityJSON, err := json.Marshal(ent)
	if err != nil {
		return err
	}

	operation := func() error {
		_, err := b.client.UpsertEntity(ctx, entityJSON, nil)
		if err != nil {
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) && respErr.StatusCode >= 500 {
				// retry on 5xx errors
				return err
			}
			// do not retry 4xx errors (bad request, conflict, etc.)
			return backoff.Permanent(err)
		}
		return nil
	}

	opBackoff := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(b.operationRetries))
	if err := backoff.Retry(operation, opBackoff); err != nil {
		return fmt.Errorf("failed to upsert key '%s' after retries: %w", entry.Key, err)
	}

	fmt.Printf("Successfully upserted key '%s'.\n", entry.Key)
	return nil
}

// Delete removes a secret by key from Azure Table Storage.
func (b *AzureTableBackend) Delete(ctx context.Context, key string) error {
	partition := partitionForKey(key)
	rkHash, _ := rowKeyHashForKey(key)

	_, err := b.client.DeleteEntity(ctx, partition, rkHash, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			// 404 Not Found means the entity was already deleted, treat as success
			fmt.Printf("Key '%s' already not found, treating as a successful deletion.\n", key)
			return nil
		}

		// Any other error is returned to caller
		fmt.Printf("Error deleting key '%s': %v\n", key, err)
		return err
	}

	fmt.Printf("Successfully deleted key '%s'.\n", key)
	return nil
}

// List returns all keys under the "secrets" partition with an optional prefix filter.
func (b *AzureTableBackend) List(ctx context.Context, prefix string) ([]string, error) {
	partition := partitionForKey(prefix)

	filter := fmt.Sprintf("PartitionKey eq '%s'", partition)
	if prefix != "" {
		ge := odataEscapeLiteral(prefix)
		lt := odataEscapeLiteral(prefix + "~")
		filter = fmt.Sprintf("%s and OrigKey ge '%s' and OrigKey lt '%s'", filter, ge, lt)
	}

	// Select only OrigKey to reduce payload
	sel := "OrigKey"
	opts := &aztables.ListEntitiesOptions{Filter: &filter, Select: &sel}
	pager := b.client.NewListEntitiesPager(opts)

	children := make(map[string]struct{}) // to avoid duplicates

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list entities: %w", err)
		}

		for _, entityJSON := range page.Entities {
			var props map[string]any
			if err := json.Unmarshal(entityJSON, &props); err != nil {
				fmt.Println("failed to unmarshal entity, skipping")
				continue
			}

			ok := false
			okKey, _ := props["OrigKey"].(string)
			if okKey == "" {
				continue
			}

			// Compute the visible child name relative to the given prefix
			visible := okKey
			if prefix != "" {
				if after, yes := strings.CutPrefix(visible, prefix); yes {
					visible = after
					ok = true
				}
			} else {
				// No prefix: we need to show only first path component
				ok = true
			}
			if !ok {
				continue
			}

			// Only take the first path component
			parts := strings.SplitN(visible, "/", 2)
			child := parts[0]
			if len(parts) > 1 {
				// It's a folder, add trailing slash
				child += "/"
			}
			children[child] = struct{}{}
		}
	}

	// Convert map to slice
	keys := make([]string, 0, len(children))
	for k := range children {
		keys = append(keys, k)
	}

	fmt.Printf("Found %d keys.\n", len(keys))
	return keys, nil
}

// ListPage implements a paginated list of keys with prefix and after parameters.
func (b *AzureTableBackend) ListPage(ctx context.Context, prefix, after string, limit int) ([]string, error) {
	partition := partitionForKey(prefix)

	// Prepare filter: only PartitionKey = "secrets"
	filter := fmt.Sprintf("PartitionKey eq '%s'", partition)
	if prefix != "" {
		ge := odataEscapeLiteral(prefix)
		lt := odataEscapeLiteral(prefix + "~")
		filter = fmt.Sprintf("%s and OrigKey ge '%s' and OrigKey lt '%s'", filter, ge, lt)
	}

	// If `after` is set, start after that key
	if after != "" {
		gt := odataEscapeLiteral(prefix + after)
		filter = fmt.Sprintf("%s and OrigKey gt '%s'", filter, gt)
	}

	// Select only OrigKey to reduce payload
	sel := "OrigKey"
	opts := &aztables.ListEntitiesOptions{Filter: &filter, Select: &sel}
	pager := b.client.NewListEntitiesPager(opts)
	keys := make([]string, 0, max(0, limit))

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list entities: %w", err)
		}

		for _, entityJSON := range page.Entities {
			var props map[string]any
			if err := json.Unmarshal(entityJSON, &props); err != nil {
				fmt.Println("failed to unmarshal entity, skipping")
				continue
			}

			ok := false
			okKey, _ := props["OrigKey"].(string)
			if okKey == "" {
				continue
			}

			// Compute the visible child name relative to the given prefix
			visible := okKey
			if prefix != "" {
				if afterPrefix, yes := strings.CutPrefix(visible, prefix); yes {
					visible = afterPrefix
					ok = true
				}
			} else {
				// No prefix: we need to show only first path component
				ok = true
			}
			if !ok {
				continue
			}

			keys = append(keys, visible)

		}
	}

	// Ensure deterministic order for paging
	sort.Strings(keys)

	// Apply 'after' cursor (strictly greater than)
	if after != "" {
		i := sort.SearchStrings(keys, after)
		// move past any equal element(s)
		for i < len(keys) && keys[i] == after {
			i++
		}
		keys = keys[i:]
	}

	// Apply limit
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	fmt.Printf("Found %d keys in ListPage.\n", len(keys))
	return keys, nil
}

// Examples we must handle:
//
// namespaces/<NS_ID>/logical/.../versions/.../<blob>
// logical/<...>/versions/.../<blob>                 (root: no namespace)
//
// Returns:
//
//	nsID: "" if there's no namespace
//	rest: everything AFTER "<nsID>/" (for namespaced) or the whole key (for root)
//	hasNS: true if the key is namespaced
func parseNamespaceIDAndRest(key string) (nsID, rest string, hasNS bool) {
	const nsPrefix = "namespaces/"
	if strings.HasPrefix(key, nsPrefix) {
		rem := strings.TrimPrefix(key, nsPrefix)
		// rem = "<nsID>/logical/..."
		parts := strings.SplitN(rem, "/", 2)
		if len(parts) < 2 || parts[0] == "" {
			return "", "", false // malformed namespaced key
		}
		return parts[0], parts[1], true
	}
	// no namespace (root)
	return "", key, false
}

// PartitionKey:
//   - if namespaced: secrets-nsID
//   - else: "secrets"
func partitionForKey(key string) string {
	partitionStatic := "secrets"
	nsID, _, hasNS := parseNamespaceIDAndRest(key)
	if hasNS {
		return safeRowKey(partitionStatic + "-" + nsID)
	}
	return partitionStatic
}

// RowKey (shortened): sha256 of the "key"
// Using full 32-byte hex keeps collision risk negligible.
// If you must shorten further, be explicit about the risk/tradeoff.
func rowKeyHashForKey(key string) (hashed string, rest string) {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]), key
}

// Escape single quotes for OData string literals.
func odataEscapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// safeRowKey will convert `/` ro `+`.
// Azure datatable does not allow certain special characters,
// therefore it is required to do the convert when trying to `PUT` entity
func safeRowKey(rowKey string) string {
	return strings.ReplaceAll(rowKey, "/", "+")
}

// revertSafeRowKey will convert `+` ro `/`.
// OpenBao keys are paths using `/` while Azure does not allow the character,
// therefore it is required to do the convert when trying to `GET` entity
func revertSafeRowKey(rowKey string) string {
	return strings.ReplaceAll(rowKey, "+", "/")
}
