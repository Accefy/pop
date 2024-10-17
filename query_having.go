package pop

import "github.com/Accefy/pop/logging"

// Having will append a HAVING clause to the query
func (q *Query) Having(condition string, args ...interface{}) *Query {
	if q.RawSQL.Fragment != "" {
		log(logging.Warn, nil, "Query is setup to use raw SQL")
		return q
	}
	q.havingClauses = append(q.havingClauses, HavingClause{condition, args})

	return q
}
