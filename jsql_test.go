// This test requires access to a postgres database named 'test' by the current os user.
package jsql

import (
	"bytes"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("postgres", "dbname=test sslmode=disable")
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}
}

func TestThatItWorks(t *testing.T) {
	_, err := db.Exec(`create temp table foo (i int, s text)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`insert into foo (i,s) values (1, 'one'), (5, 'five'), (7, 'seven'), (9, 'nine')`)
	if err != nil {
		t.Fatal(err)
	}

	q, err := Q(db, "SELECT i AS int, s AS string FROM foo WHERE i > ${first} AND NOT s LIKE ${pat}")
	if err != nil {
		t.Fatal(err)
	}

	var b bytes.Buffer

	n, err := q(map[string]interface{}{"first": 3, "pat": `%eve%`}, &b)
	if err != nil || n != 2 {
		t.Error(n, err)
	}

	xpct := `[
{"int":5,"string":"five"},
{"int":9,"string":"nine"}
]`

	if b.String() != xpct {
		t.Errorf("Expected %q, got %q", xpct, b.String())
	}

	_, err = db.Exec(`drop table foo`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedArg(t *testing.T) {
	_, err := db.Exec(`create temp table foo (i int, s text)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`insert into foo (i,s) values (1, 'one'), (5, 'five'), (7, 'seven'), (9, 'nine')`)
	if err != nil {
		t.Fatal(err)
	}

	q, err := Q(db, "SELECT i AS int, s AS string, ${first}::int as arg FROM foo WHERE i > ${first} AND NOT s LIKE ${pat}")
	if err != nil {
		t.Fatal(err)
	}

	var b bytes.Buffer

	n, err := q(map[string]interface{}{"first": 3, "pat": `%eve%`}, &b)
	if err != nil || n != 2 {
		t.Error(n, err)
	}

	xpct := `[
{"arg":3,"int":5,"string":"five"},
{"arg":3,"int":9,"string":"nine"}
]`

	if b.String() != xpct {
		t.Errorf("Expected %s,\ngot %s", xpct, b.String())
	}

	_, err = db.Exec(`drop table foo`)
	if err != nil {
		t.Fatal(err)
	}

}
