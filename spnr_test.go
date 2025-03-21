package spnr_test

import (
	"context"
	"testing"

	"github.com/acudac-com/spnr-go"
	"go.alis.build/alog"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

func TestCreateTestTable(t *testing.T) {
	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: "projects/local/instances/test-instance/databases/dev",
		Statements: []string{
			`CREATE TABLE Users (
				id STRING(100),
				name STRING(MAX),
				age INT64,
			) PRIMARY KEY (id)`,
		},
	})
	if err != nil {
		t.Fatalf("UpdateDatabaseDdl: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

type User struct {
	id   string
	name string
	age  int64
}

var Db *spnr.Db
var UserTbl *spnr.Table[*User, string]
var ctx = context.Background()

func init() {
	var err error
	Db, err = spnr.NewDbClient(ctx, "projects/local/instances/test-instance/databases/dev", "somerole")
	if err != nil {
		panic(err)
	}
	keyConverter := func(id string) spanner.Key {
		return spanner.Key{id}
	}
	readConverter := func(row *spanner.Row) (*User, error) {
		user := &User{}
		for i, col := range row.ColumnNames() {
			switch col {
			case "id":
				row.Column(i, &user.id)
			case "name":
				row.Column(i, &user.name)
			case "age":
				row.Column(i, &user.age)
			}
		}
		return user, nil
	}

	writeConverter := func(user *User) (map[string]any, error) {
		return map[string]any{
			"id":   user.id,
			"name": user.name,
			"age":  user.age,
		}, nil
	}
	UserTbl, err = spnr.NewTableClient(Db, "Users", []string{"id", "name", "age"}, keyConverter, readConverter, writeConverter)
	if err != nil {
		alog.Warnf(ctx, "Could not create UserTbl: %v. Remember to run TestCreateTestTable", err)
	}
}

func TestCreate(t *testing.T) {
	user := &User{
		id:   "asdf",
		name: "Daniel",
		age:  27,
	}
	if err := UserTbl.Create(ctx, nil, user); err != nil {
		t.Fatal(err)
	}
}

func TestUpdate(t *testing.T) {
	user := &User{
		id:   "asdf",
		name: "Daniel van Niekerk",
		age:  27,
	}
	if err := UserTbl.Update(ctx, nil, user); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	if err := UserTbl.Delete(ctx, nil, "asdf"); err != nil {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	user, err := UserTbl.Read(ctx, nil, "asdf")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(user)
}

func TestBatchRead(t *testing.T) {
	users, err := UserTbl.BatchRead(ctx, nil, []string{"asdf", "qwer"})
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range users {
		t.Log(user)
	}
}

func TestQuery(t *testing.T) {
	users, err := UserTbl.Query(ctx, nil, &spnr.QueryOpts{
		Where: "age < 30",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range users {
		t.Log(user)
	}
}
