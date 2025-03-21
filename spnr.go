package spnr

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
//   - ctx: The context
//   - database: The full name of the database in the format 'projects/*/instances/*/databases/*'
//   - role: The database role to use for fine grained database access.
func NewDbClient(ctx context.Context, database, role string) (*Db, error) {
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
type Table[RowT any, KeyT any] struct {
	Db             *Db
	name           string
	colSet         map[string]bool
	readConverter  func(r *spanner.Row) (RowT, error)
	writeConverter func(row RowT) (map[string]any, error)
	keyConverter   func(key KeyT) spanner.Key
}

// Returns a table client.
//   - db: A database client created with spnr.NewDbClient
//   - table: The table name
//   - columns: All the columns in the table including the key(s)
//   - rowConverter: A function that takes in a spanner row and returns the generic type of this client
func NewTableClient[RowT any, KeyT any](db *Db, table string, columns []string, keyConverter func(key KeyT) spanner.Key, readConverter func(r *spanner.Row) (RowT, error), writeConverter func(row RowT) (map[string]any, error)) (*Table[RowT, KeyT], error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if table == "" {
		return nil, errors.New("invalid table name")
	}
	if len(columns) == 0 {
		return nil, errors.New("columns not specified")
	}
	if readConverter == nil {
		return nil, errors.New("rowConverter not specified")
	}
	colSet := map[string]bool{}
	for _, col := range columns {
		colSet[col] = true
	}
	return &Table[RowT, KeyT]{
		Db:             db,
		name:           table,
		colSet:         colSet,
		keyConverter:   keyConverter,
		readConverter:  readConverter,
		writeConverter: writeConverter,
	}, nil
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

// Returns a read-only spanner transaction to read multiple rows at the exact same point in time.
func (d *Db) ReadOnlyTxn() *spanner.ReadOnlyTransaction {
	return d.Client.ReadOnlyTransaction()
}

// Executes a read-write transaction, with retries as necessary.
//
// The function f will be called one or more times. It must not maintain any state between calls.
//
// If the transaction cannot be committed or if f returns an ABORTED error, ReadWriteTransaction will call f again. It will continue to call f until the transaction can be committed or the Context times out or is cancelled. If f returns an error other than ABORTED, ReadWriteTransaction will abort the transaction and return the error.
//
// To limit the number of retries, set a deadline on the Context rather than using a fixed limit on the number of attempts. ReadWriteTransaction will retry as needed until that deadline is met.
//
// See https://godoc.org/cloud.google.com/go/spanner#ReadWriteTransaction for more details.
func (d *Db) ReadWriteTxn(ctx context.Context, f func(context.Context, *spanner.ReadWriteTransaction) error) error {
	if _, err := d.Client.ReadWriteTransaction(ctx, f); err != nil {
		return err
	}
	return nil
}

// Reads the specified row.
// If no txn given, a ReadOnly txn is created. Its closed after returning the result.
// If cols not specified, all columns are read.
func (t *Table[RowT, KeyT]) Read(ctx context.Context, txn ReadTxn, key KeyT, cols ...string) (RowT, error) {
	if txn == nil {
		tempTxn := t.Db.Client.ReadOnlyTransaction()
		defer tempTxn.Close()
		txn = tempTxn
	}

	if len(cols) == 0 {
		for col := range t.colSet {
			cols = append(cols, col)
		}
	}

	var nilRow RowT
	row, err := txn.ReadRow(ctx, t.name, t.keyConverter(key), cols)
	if err != nil {
		if errors.Is(err, spanner.ErrRowNotFound) {
			return nilRow, status.Errorf(codes.NotFound, "%v not found", key)
		} else {
			return nilRow, status.Errorf(codes.Internal, "reading spanner row: %v", err)
		}
	}
	return t.readConverter(row)
}

// Reads the specified rows. Does not fail if a row is not found.
// If no txn given, a ReadOnly txn is created. Its closed after returning the result.
// If cols not specified, all columns are read.
func (t *Table[RowT, KeyT]) BatchRead(ctx context.Context, txn ReadTxn, keys []KeyT, cols ...string) ([]RowT, error) {
	if txn == nil {
		tempTxn := t.Db.Client.ReadOnlyTransaction()
		defer tempTxn.Close()
		txn = tempTxn
	}

	if len(cols) == 0 {
		for col := range t.colSet {
			cols = append(cols, col)
		}
	}

	rows := []RowT{}
	do := func(r *spanner.Row) error {
		row, err := t.readConverter(r)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	}

	spannerKeys := []spanner.Key{}
	for _, key := range keys {
		spannerKeys = append(spannerKeys, t.keyConverter(key))
	}

	it := txn.Read(ctx, t.name, spanner.KeySetFromKeys(spannerKeys...), cols)
	if err := it.Do(do); err != nil {
		return nil, err
	}
	return rows, nil
}

// Creates a new row.
//   - ctx: context
//   - txn: optional spanner ReadWrite txn; if none given, one is created and also closed after the create
//   - row: the new row's data
func (t *Table[RowT, KeyT]) Create(ctx context.Context, txn WriteTxn, row RowT) error {
	// convert row to map and validate
	m, err := t.writeConverter(row)
	if err != nil {
		return err
	}
	for col := range m {
		if _, ok := t.colSet[col]; !ok {
			return status.Errorf(codes.Internal, "writeConverter returned %s, which is not a valid column in %s", col, t.name)
		}
	}

	// build up cols and values
	columns := []string{}
	values := []any{}
	for col, val := range m {
		columns = append(columns, col)
		values = append(values, val)
	}

	// run insert mutation
	insertMutation := spanner.Insert(t.name, columns, values)
	if err := t.Db.Mutate(ctx, txn, insertMutation); err != nil {
		return status.Errorf(codes.Internal, "creating spanner row: %v", err)
	}
	return nil
}

// Update an existing row.
//   - ctx: context
//   - txn: optional spanner ReadWrite txn; if none given, one is created and also closed after the update
//   - row: the existing row's data
//   - cols: optional list of columns to limit the update to.
func (t *Table[RowT, KeyT]) Update(ctx context.Context, txn WriteTxn, row RowT, cols ...string) error {
	// validate cols if any and build map
	colSet := map[string]bool{}
	if len(cols) > 0 {
		for _, col := range cols {
			if _, ok := t.colSet[col]; !ok {
				return status.Errorf(codes.Internal, "update: %s is not a valid column to limit update to in %s", col, t.name)
			}
			colSet[col] = true
		}
	} else {
		colSet = t.colSet
	}

	// convert row to map
	m, err := t.writeConverter(row)
	if err != nil {
		return err
	}
	for col := range m {
		if _, ok := t.colSet[col]; !ok {
			return status.Errorf(codes.Internal, "writeConverter returned %s, which is not a valid column in %s", col, t.name)
		}
	}

	// build up cols and values
	columns := []string{}
	values := []any{}
	for col, value := range m {
		if _, ok := colSet[col]; ok {
			columns = append(columns, col)
			values = append(values, value)
		}
	}

	// run update mutation
	updateMutation := spanner.Update(t.name, columns, values)
	if err := t.Db.Mutate(ctx, txn, updateMutation); err != nil {
		return status.Errorf(codes.Internal, "updating spanner row: %v", err)
	}
	return nil
}

// Delete the row at the given key.
// Creates a new ReadWrite transaction if none given.
func (t *Table[RowT, KeyT]) Delete(ctx context.Context, txn WriteTxn, key KeyT) error {
	spannerKey := t.keyConverter(key)
	deleteMutation := spanner.Delete(t.name, spanner.KeySetFromKeys(spannerKey))
	if err := t.Db.Mutate(ctx, txn, deleteMutation); err != nil {
		return status.Errorf(codes.Internal, "deleting spanner rows: %v", err)
	}
	return nil
}

// Delete the rows at the given keys.
// Creates a new ReadWrite transaction if none given.
func (t *Table[RowT, KeyT]) BatchDelete(ctx context.Context, txn WriteTxn, keys ...KeyT) error {
	if len(keys) == 0 {
		return status.Errorf(codes.Internal, "no keys specified in BatchDelete")
	}

	spannerKeys := []spanner.Key{}
	for _, key := range keys {
		spannerKeys = append(spannerKeys, t.keyConverter(key))
	}

	deleteMutation := spanner.Delete(t.name, spanner.KeySetFromKeys(spannerKeys...))
	if err := t.Db.Mutate(ctx, txn, deleteMutation); err != nil {
		return status.Errorf(codes.Internal, "deleting spanner rows: %v", err)
	}
	return nil
}

// A column to sort on
type SortedColumn struct {
	Col        string
	Descending bool
}

type QueryOpts struct {
	// The columns to select
	Cols []string
	// The sorting to apply
	SortCols []*SortedColumn
	// The max number of rows to return
	Limit int32
	// Start after these nr of rows; useful for pagination
	Offset int64
	// A filter to apply; preceeded by "WHERE" keyword
	Where string
}

// Queries the rows in the table using the parameters specifed in opts.
// Creates a new ReadOnly txn if none given.
func (t *Table[RowT, KeyT]) Query(ctx context.Context, txn ReadTxn, opts *QueryOpts) ([]RowT, error) {
	if opts == nil {
		opts = &QueryOpts{}
	}

	// create txn if none given
	if txn == nil {
		t := t.Db.Client.ReadOnlyTransaction()
		defer t.Close()
		txn = t
	}

	// Initialize query
	query := fmt.Sprintf("SELECT * FROM %s", t.name)
	if len(opts.Cols) > 0 {
		wrappedColNames := []string{}
		for _, col := range opts.Cols {
			wrappedColNames = append(wrappedColNames, fmt.Sprintf("`%s`", col))
		}
		query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(wrappedColNames, ","), t.name)
	}

	// Add where clause if provided
	if opts.Where != "" {
		query += " WHERE " + opts.Where
	}

	// Add sort cols if provided
	if len(opts.SortCols) > 0 {
		query += " ORDER BY "
		sortColumns := make([]string, 0, len(opts.SortCols))
		for _, sortedCol := range opts.SortCols {
			order := "ASC"
			if sortedCol.Descending {
				order = "DESC"
			}
			sortColumns = append(sortColumns, fmt.Sprintf("%s %s", sortedCol.Col, order))
		}
		query += strings.Join(sortColumns, ", ")
	}

	// Add limit if provided
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %v", opts.Limit)
	}

	// Add offset if provided
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %v", opts.Offset)
	}

	// Setup do function
	rows := []RowT{}
	do := func(r *spanner.Row) error {
		row, err := t.readConverter(r)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	}

	// Run query
	it := txn.Query(ctx, spanner.NewStatement(query))
	if err := it.Do(do); err != nil {
		return nil, status.Errorf(codes.Internal, "querying table %s: %v", t.name, err)
	}
	return rows, nil
}
