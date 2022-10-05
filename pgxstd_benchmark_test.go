package sqlbenchmark_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func init() {
	db := newDB(nil, "pgx")

	for i := 0; i < 4; i++ {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS t%d", i+1))
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(fmt.Sprintf("CREATE TABLE t%d (col1 text, col2 integer, col3 float, col4 timestamp)", i+1))
		if err != nil {
			panic(err)
		}
		prep, err := db.Prepare(fmt.Sprintf("INSERT INTO t%d VALUES ($1, $2, $3, $4)", i+1))
		if err != nil {
			panic(err)
		}
		defer prep.Close()
		for i := 0; i < 10000; i++ {
			_, err := prep.Exec(fmt.Sprintf("text:%d", i+1), i+1, float64(i)/3.14, time.Now())
			if err != nil {
				panic(err)
			}
		}
	}

	// Nested Loop  (cost=187.25..1396.83 rows=54872 width=52)
	row := db.QueryRow("EXPLAIN "+query, "text:1", 1)
	if err := row.Err(); err != nil {
		panic(err)
	}
	head1 := ""
	if err := row.Scan(&head1); err != nil {
		panic(err)
	}
	fmt.Println(head1)
}

func newDB(t testing.TB, driverName string) *sql.DB {
	db, err := sql.Open(driverName, "postgres://postgres:secret@127.0.0.1:5432/postgres?sslmode=disable")
	if err != nil {
		if t != nil {
			t.Fatal(err)
		} else {
			panic(err)
		}
	}

	return db
}

type Object struct {
	Col1 string
	Col2 string
	Col3 string
	Col4 string
}

const query = `
    SELECT
      t1.col1, t1.col2, t1.col3, t1.col4 
    FROM t1
    JOIN t2 on t1.col1 = t2.col1
    JOIN t3 on t2.col1 = t3.col1
    WHERE
      t1.col1 = $1
    AND
      t3.col1 IN (SELECT t4.col1 FROM t4 WHERE t4.col2 = $2 ORDER BY t4.col4)
`

func BenchmarkSqlPgxstdSimpleSelect(b *testing.B) {
	db := newDB(b, "pgx")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(query, fmt.Sprintf("text:%d", i+1), i+1)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			obj := new(Object)
			if err := rows.Scan(&obj.Col1, &obj.Col2, &obj.Col3, &obj.Col4); err != nil {
				b.Fatal(err)
			}
		}

		rows.Close()
	}
}

func BenchmarkSqlPgxstdPrepareSelect(b *testing.B) {
	db := newDB(b, "pgx")

	stmt, err := db.Prepare(query)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query(fmt.Sprintf("text:%d", i+1), i+1)
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			obj := new(Object)
			if err := rows.Scan(&obj.Col1, &obj.Col2, &obj.Col3, &obj.Col4); err != nil {
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}
