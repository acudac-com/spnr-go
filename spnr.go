package spnr

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	spapi "cloud.google.com/go/spanner/apiv1"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Interface satisfied by a ReadOnly and ReadWrite transactions.
type ReadTxn interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

// Interface satisfied by a ReadWrite transaction.
type WriteTxn interface {
	BufferWrite(ms []*spanner.Mutation) error
}

// A database.
type Db struct {
	Name   string
	Client *spanner.Client
}

// Returns a database client that only retries transactions if "Unavailable" is returned.
//   - database: The full name of the database in the format 'projects/*/instances/*/databases/*'
//   - role: The database role to use for fine grained database access.
func NewDbClient(database, role string) (*Db, error) {
	ctx := context.Background()
	co := &spapi.CallOptions{
		ExecuteSql: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        5000 * time.Millisecond,
					Multiplier: 1.5,
				})
			}),
		},
	}
	client, err := spanner.NewClientWithConfig(ctx, database, spanner.ClientConfig{DatabaseRole: role, CallOptions: co, DisableNativeMetrics: true})
	if err != nil {
		return nil, fmt.Errorf("creating spanner client to %s with role=%s: %v", database, role, err)
	}
	return &Db{
		Name:   database,
		Client: client,
	}, nil
}

// A table.
type Table[RowT *any, KeyT any] struct {
	db           *Db
	name         string
	columns      []string
	rowConverter func(r *spanner.Row) (RowT, error)
	keyConverter func(KeyT) (spanner.Key, error)
}

// Returns a table client.
//   - db: A database client created with spnr.NewDbClient
//   - table: The table name
//   - rowConverter: A function that takes in a spanner row and returns the generic type of this client
func NewTableClient[RowT *any, KeyT any](db *Db, table string, columns []string, rowConverter func(r *spanner.Row) (RowT, error), keyConverter func(key KeyT) (spanner.Key, error)) *Table[RowT, KeyT] {
	return &Table[RowT, KeyT]{
		db:           db,
		name:         table,
		columns:      columns,
		rowConverter: rowConverter,
		keyConverter: keyConverter,
	}
}

// Apply one/more row mutations on a Db.
// Creates a new ReadWrite transaction if none given.
func (d *Db) Mutate(ctx context.Context, txn WriteTxn, mutations ...*spanner.Mutation) error {
	var err error
	if txn == nil {
		_, err = d.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite(mutations)
		})
	} else {
		err = txn.BufferWrite(mutations)
	}
	return err
}

// Reads the specified row.
// If no txn given, one is created which is closed after returning the result.
func (t *Table[RowT, KeyT]) Read(ctx context.Context, txn ReadTxn, key KeyT, cols ...string) (RowT, error) {
	if txn == nil {
		tempTxn := t.db.Client.ReadOnlyTransaction()
		defer tempTxn.Close()
		txn = tempTxn
	}

	spannerKey, err := t.keyConverter(key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to construct spanner key from %v: %v", key, err)
	}
	if len(cols) == 0 {
		cols = t.columns
	}

	row, err := txn.ReadRow(ctx, t.name, spannerKey, cols)
	if err != nil {
		if errors.Is(err, spanner.ErrRowNotFound) {
			return nil, status.Errorf(codes.NotFound, "%v not found", key)
		} else {
			return nil, status.Errorf(codes.Internal, "reading spanner row: %v", err)
		}
	}
	return t.rowConverter(row)
}

// Reads the specified rows.
// If no txn given, one is created which is closed after returning the result.
func (t *Table[RowT, KeyT]) BatchRead(ctx context.Context, txn ReadTxn, keys []KeyT, cols ...string) ([]RowT, error) {
	if txn == nil {
		tempTxn := t.db.Client.ReadOnlyTransaction()
		defer tempTxn.Close()
		txn = tempTxn
	}

	spannerKeys := []spanner.Key{}
	for _, key := range keys {
		spannerKey, err := t.keyConverter(key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to construct spanner key from %v: %v", key, err)
		}
		spannerKeys = append(spannerKeys, spannerKey)
	}

	rows := []RowT{}
	do := func(r *spanner.Row) error {
		row, err := t.rowConverter(r)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	}

	it := txn.Read(ctx, t.name, spanner.KeySetFromKeys(spannerKeys...), cols)
	if err := it.Do(do); err != nil {
		return nil, err
	}
	return rows, nil
}
