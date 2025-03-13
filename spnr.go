package spnr

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	spapi "cloud.google.com/go/spanner/apiv1"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
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
	Name       string
	SpannerCli *spanner.Client
}

// Returns a database client that only retries transactions if "Unavailable" is returned.
//   - database: The full name of the database in the format 'projects/*/instances/*/databases/*'
//   - role: The database role to use for fine grained database access.
func NewDbClient(database, role string) *Db {
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
	spannerCli, err := spanner.NewClientWithConfig(ctx, database, spanner.ClientConfig{DatabaseRole: role, CallOptions: co, DisableNativeMetrics: true})
	if err != nil {
		panic(fmt.Sprintf("connecting to spanner database (%s): %v", database, err))
	}
	return &Db{
		Name:       database,
		SpannerCli: spannerCli,
	}
}

// A table.
type Table[T any] struct {
	rowConverter func(r *spanner.Row) (T, error)
	Db           *Db
}

// Returns a table client.
//   - db: A database client created with spnr.NewDbClient
//   - table: The table name
//   - rowConverter: A function that takes in a spanner row and returns the generic type of this client
func NewTableClient[T any](db *Db, table string, rowConverter func(r *spanner.Row) (T, error)) *Table[T] {
	return &Table[T]{
		rowConverter: rowConverter,
		Db:           db,
	}
}
