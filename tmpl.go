package jsql

import (
	"database/sql"
	"html/template"
	"io"
)

// A QueryTmplFunc is the type of function returned by QTmpl.
// It takes a template, in addition to a writer.  The template
// is invoked with the following argument:
//     struct {
//         Args: args                     // the arguments passed in
//         Results: <-chan []interface{}   // a chanel of records corresponding to the querys results
//     }
//
type QueryTmplFunc func(args map[string]interface{}, tmpl template.Template, w io.Writer) error

// QTmpl is like Q, but it returns a QueryTmplFunc
func QTmpl(db *sql.DB, query string) (QueryTmplFunc, error) {
	q, argn := rewrite(query)
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}
	return func(args map[string]interface{}, tmpl template.Template, w io.Writer) error {
		var argv []interface{}
		for _, v := range argn {
			argv = append(argv, args[v])
		}
		rows, err := stmt.Query(argv...)
		if err != nil {
			return err
		}
		defer rows.Close()
		retn, err := rows.Columns()
		if err != nil {
			return err
		}

		datach, errch := make(chan []interface{}), make(chan error, 1)

		go func() {
			errch <- tmpl.Execute(w, struct {
				Args    map[string]interface{}
				Results <-chan []interface{}
			}{args, datach})
		}()

		retvv := make([]interface{}, len(retn))
		for rows.Next() {
			retv := make([]interface{}, len(retn))
			for i := range retv {
				retvv[i] = &retv[i]
			}
			if err := rows.Scan(retvv...); err != nil {
				close(datach)
				return err
			}
			// convert []byte to strings
			for i, v := range retv {
				if vv, ok := v.([]byte); ok {
					retv[i] = string(vv)
				}
			}

			select {
			case err := <-errch:
				// early exit from the template
				return err
			case datach <- retv:
				// nix
			}
		}
		close(datach)                      // will exit the template
		terr := <-errch                    // wait for it, or it may not be done writing
		if err := rows.Err(); err != nil { // db error trumps
			return err
		}
		return terr
	}, nil
}
