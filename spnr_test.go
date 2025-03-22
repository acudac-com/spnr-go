package spnr_test

import (
	"context"
	"os"
	"testing"

	"github.com/acudac-com/spnr-go"
	"go.alis.build/alog"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

var DbName = os.Getenv("SPANNER_DATABASE")

func TestCreateTestTable(t *testing.T) {
	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: DbName,
		Statements: []string{
			`CREATE TABLE spnr_go_Example (
				key STRING(100),
				name STRING(100),
				age INT64,
			) PRIMARY KEY (key)`,
		},
	})
	if err != nil {
		t.Fatalf("UpdateDatabaseDdl: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

type Example struct {
	key  string
	name string
	age  int64
}

var Db *spnr.Db
var ExampleTbl *spnr.Table[*Example, string]
var ctx = context.Background()

func init() {
	var err error
	Db, err = spnr.NewDbClient(ctx, DbName, "")
	if err != nil {
		panic(err)
	}
	keyConverter := func(id string) spanner.Key {
		return spanner.Key{id}
	}
	readConverter := func(row *spanner.Row) (*Example, error) {
		user := &Example{}
		for i, col := range row.ColumnNames() {
			switch col {
			case "key":
				row.Column(i, &user.key)
			case "name":
				row.Column(i, &user.name)
			case "age":
				row.Column(i, &user.age)
			}
		}
		return user, nil
	}

	writeConverter := func(user *Example) (map[string]any, error) {
		return map[string]any{
			"key":  user.key,
			"name": user.name,
			"age":  user.age,
		}, nil
	}
	ExampleTbl, err = spnr.NewTableClient(Db, "spnr_go_Example", []string{"key", "name", "age"}, keyConverter, readConverter, writeConverter)
	if err != nil {
		alog.Warnf(ctx, "Could not create ExampleTbl: %v. Remember to run TestCreateTestTable", err)
	}
}

func TestCreate(t *testing.T) {
	user := &Example{
		key:  "asdf",
		name: "Daniel",
		age:  27,
	}
	if err := ExampleTbl.Create(ctx, nil, user); err != nil {
		t.Fatal(err)
	}
}

func TestUpdate(t *testing.T) {
	user := &Example{
		key:  "asdf",
		name: "Daniel van Niekerk",
		age:  27,
	}
	if err := ExampleTbl.Update(ctx, nil, user); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	if err := ExampleTbl.Delete(ctx, nil, "asdf"); err != nil {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	user, err := ExampleTbl.Read(ctx, nil, "asdf")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(user)
}

func TestBatchRead(t *testing.T) {
	users, err := ExampleTbl.BatchRead(ctx, nil, []string{"asdf", "qwer"})
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range users {
		t.Log(user)
	}
}

func TestQuery(t *testing.T) {
	users, err := ExampleTbl.Query(ctx, nil, &spnr.QueryOpts{
		Where: "age < 30",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range users {
		t.Log(user)
	}
}
