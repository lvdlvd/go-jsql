// Package jsql provides glue between database/sql and json.
package jsql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"regexp"

	"github.com/gorilla/mux"
)

const jsonContentType = "application/json;charset=UTF-8"

var reVars = regexp.MustCompile(`\${([^}]+)}`)

// PositionalQueryVars controls wether
// the generated query strings will use '?'
// or $1, $2... syntax.
var PositionalQueryVars = true

// TODO(lvd) less simplistic parsing of query.  worry about quoting etc.
func rewrite(q string) (qq string, varnames []string) {
	idx := reVars.FindAllStringSubmatchIndex(q, -1)
	l := 0
	var pos = map[string]int{}
	var b bytes.Buffer
	for _, v := range idx {
		b.WriteString(q[l:v[0]])
		l = v[1]
		name := q[v[2]:v[3]]
		if PositionalQueryVars {
			if _, ok := pos[name]; !ok {
				varnames = append(varnames, name)
				pos[name] = len(varnames)
			}
			b.WriteString(fmt.Sprintf("$%d", pos[name]))
		} else {
			varnames = append(varnames, name)
			b.WriteString("?")
		}
	}
	b.WriteString(q[l:])
	return b.String(), varnames
}

// TODO:make Q send records on a channel, and provide json/bson as encoders
// then have the Handler switch returned encoding depending on http headers

// A queryfunc is the type of function generated by Q.
// It executes a query substituting arguments from the provided args map,
// and writes a JSON array of objects to the provided writer.
// It returns the number of array elements written and any error that may have occured.
//
// If w is an http.ResponseWriter (has a Header() http.Header method), then it will set
// the Content-Type header to application/json;charset=UTF-8 before the first write.
//
// If n == 0, nothing, not even the opening bracket, will have been written to w,
// and the header will not have been set, meaning it is not too late to call e.g.
//
//    http.Error(w, err.Error(), http.StatusInternalError)
//
// otherwise the error will have to be tacked on to the already produced output or
// reported out of band.  If any elements have been written the queryfunc will
// always generate a proper closing bracket.
type QueryFunc func(args map[string]interface{}, w io.Writer) (n int, err error)

// Q builds a function that can execute the query on a database given
// a map of parameters, and writes the result as json to a writer.
//
// The query must be of the form 'SELECT foo FROM bar WHERE x = ${varname}'
// that is, any query accepted by the driver that will provide db,
// but with the normal '$1, $2...' parameters replace by ${varname}.
//
// The generated function will extract the corresponding values from the args map
// and supply them to the db.Stmt.Execute method in the correct order.
// The query will be rewritten to use $1, $2 .. etc.
//
// The generated json will be an array of objects, one per row where
// each row object has keys named after the columns in the SQL query.
//
func Q(db *sql.DB, query string) (QueryFunc, error) {
	q, argn := rewrite(query)
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}
	return func(args map[string]interface{}, w io.Writer) (n int, err error) {
		var argv []interface{}
		for _, v := range argn {
			argv = append(argv, args[v])
		}
		rows, err := stmt.Query(argv...)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		retn, err := rows.Columns()
		if err != nil {
			return 0, err
		}

		var (
			retv  = make([]interface{}, len(retn))
			retvv = make([]interface{}, len(retn))
			retm  = make(map[string]interface{})
		)
		for i := range retv {
			retvv[i] = &retv[i]
			retm[retn[i]] = &retv[i]
		}
		for rows.Next() {
			if err := rows.Scan(retvv...); err != nil {
				return n, err
			}
			// convert []byte to strings, because json uuencodes []byte
			for i, v := range retv {
				if vv, ok := v.([]byte); ok {
					retv[i] = string(vv)
				}
			}

			b, err := json.Marshal(retm)
			if err != nil {
				return n, err
			}

			if n == 0 {
				if rw, ok := w.(http.ResponseWriter); ok {
					rw.Header().Set("Content-type", jsonContentType)
				}
				w.Write([]byte("[\n"))
				defer w.Write([]byte("\n]"))
			} else {
				w.Write([]byte(",\n"))
			}

			w.Write(b)
			n++
		}

		return n, rows.Err()
	}, nil
}

// Handler is a convenience wrapper around MkHandler
// that will die on error.
func Handler(db *sql.DB, query string) http.Handler {
	h, err := MkHandler(db, query)
	if err != nil {
		log.Panicf("MkHandler(%q): %v", query, err)
	}
	return h
}

// MkHandler produces a http.Handler that takes arguments from
// the request, and runs Q, producing a nice error message if needed.
//
// The arguments are taken from the request's formvalues or any
// input json object, depending on the requests content type, merged with
// the "github.com/gorilla/mux".Vars(r), which take precedence
// in case of name conflicts.
func MkHandler(db *sql.DB, query string) (http.Handler, error) {
	_, names := rewrite(query)

	qf, err := Q(db, query)
	if err != nil {
		return nil, err
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := make(map[string]interface{})

		var jsonargs map[string]interface{}
		if r.Method == "POST" || r.Method == "PUT" {
			ct := r.Header.Get("Content-Type")
			if ct == "" {
				ct = "application/octet-stream"
			}
			ct, _, err = mime.ParseMediaType(ct)
			if ct == "application/json" {
				defer r.Body.Close()
				if err := json.NewDecoder(io.LimitReader(r.Body, 128<<10)).Decode(&jsonargs); err != nil {
					if err != io.EOF { // empty body is allowed
						http.Error(w, fmt.Sprintf("Can't decode json request: %v", err), http.StatusBadRequest)
						return
					}
				}
			}
		}
		muxargs := mux.Vars(r)
		// TODO harden against malicious input
		// for now we just rely on precedence: muxargs trum the others
		// but this still allows arbitrary values to end up in the query parameters
		for _, n := range names {
			if v, ok := muxargs[n]; ok {
				args[n] = v
				continue
			}
			if v := r.FormValue(n); v != "" {
				args[n] = v
				continue
			}
			if v, ok := jsonargs[n]; ok {
				args[n] = v
				continue
			}
		}
		// TBD: what if not all names set? can postgres $n deal with nil?
		n, err := qf(args, w)
		if err != nil {
			if n == 0 {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			log.Print(r.Method, r.URL, ": ", err)
		}
	}), nil
}
