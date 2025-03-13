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

// Interface satisfied by ReadOnly and ReadWrite transactions.
type ReadTxn interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

// Interface satisfied by spanner's ReadWrite transaction.
type WriteTxn interface {
	BufferWrite(ms []*spanner.Mutation) error
}

// A database.
type Db struct {
	Name       string
	SpannerCli *spanner.Client
}

// Returns a new database instance, containing a client connection using spanner's database roles.
// The connection is also setup to only retry spanner transactions if "Unavailable" is returned.
//   - database: The full name of the spanner database in the format 'projects/*/instances/*/databases/*'
//   - role: The database role to use for fine grained database access.
func NewDb(database, role string) *Db {
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
