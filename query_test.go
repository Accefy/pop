package pop

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func Test_Where(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	a := require.New(t)
	m := NewModel(new(Enemy), context.Background())

	q := PDB.Where("id = ?", 1)
	sql, args := q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies WHERE id = ?"), sql)
	a.Equal([]interface{}{1}, args)

	q.Where("first_name = ? and last_name = ?", "Mark", "Bates")
	sql, args = q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies WHERE id = ? AND first_name = ? and last_name = ?"), sql)
	a.Equal([]interface{}{1, "Mark", "Bates"}, args)

	q = PDB.Where("name = ?", "Mark 'Awesome' Bates")
	sql, args = q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies WHERE name = ?"), sql)
	a.Equal([]interface{}{"Mark 'Awesome' Bates"}, args)

	q = PDB.Where("name = ?", "'; truncate users; --")
	sql, _ = q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies WHERE name = ?"), sql)

	q = PDB.Where("id is not null") // no args
	sql, args = q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies WHERE id is not null"), sql)
	a.Equal([]interface{}{}, args)
}

func Test_Where_In(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	r := require.New(t)
	transaction(func(tx *Connection) {
		u1 := &Song{Title: "A"}
		u2 := &Song{Title: "B"}
		u3 := &Song{Title: "C"}
		r.NoError(tx.Create(nil, u1))
		r.NoError(tx.Create(nil, u2))
		r.NoError(tx.Create(nil, u3))

		var songs []Song
		err := tx.Where("id in (?)", u1.ID, u3.ID).All(nil, &songs)
		r.NoError(err)
		r.Len(songs, 2)
	})
}

func Test_Where_In_Slice(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	r := require.New(t)
	transaction(func(tx *Connection) {
		u1 := &Song{Title: "A"}
		u2 := &Song{Title: "A"}
		u3 := &Song{Title: "A"}
		r.NoError(tx.Create(nil, u1))
		r.NoError(tx.Create(nil, u2))
		r.NoError(tx.Create(nil, u3))

		Debug = true
		defer func() { Debug = false }()

		var songs []Song
		err := tx.Where("id in (?)", []uuid.UUID{u1.ID, u3.ID}).Where("title = ?", "A").All(nil, &songs)
		r.NoError(err)
		r.Len(songs, 2)

		// especially https://github.com/gobuffalo/pop/issues/699
		err = tx.Where("id in (?)", []uuid.UUID{u1.ID, u3.ID}).Delete(nil, &Song{})
		r.NoError(err)

		var remainingSongs []Song
		r.NoError(tx.All(nil, &remainingSongs))
		r.Len(remainingSongs, 1)
	})
}

func Test_Where_In_One(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	r := require.New(t)
	transaction(func(tx *Connection) {
		u1 := &Song{Title: "A"}
		u2 := &Song{Title: "B"}
		u3 := &Song{Title: "C"}
		r.NoError(tx.Create(nil, u1))
		r.NoError(tx.Create(nil, u2))
		r.NoError(tx.Create(nil, u3))

		Debug = true
		defer func() { Debug = false }()

		var songs []Song
		err := tx.Where("id in (?)", u1.ID).All(nil, &songs)
		r.NoError(err)
		r.Len(songs, 1)
	})
}

func Test_Where_In_Complex(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	r := require.New(t)
	transaction(func(tx *Connection) {
		u1 := &Song{Title: "A"}
		u2 := &Song{Title: "A"}
		u3 := &Song{Title: "A"}
		r.NoError(tx.Create(nil, u1))
		r.NoError(tx.Create(nil, u2))
		r.NoError(tx.Create(nil, u3))

		var songs []Song
		err := tx.Where("id in (?)", u1.ID, u3.ID).Where("title = ?", "A").All(nil, &songs)
		r.NoError(err)
		r.Len(songs, 2)
	})
}

func Test_Where_In_Complex_One(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	r := require.New(t)
	transaction(func(tx *Connection) {
		u1 := &Song{Title: "A"}
		u2 := &Song{Title: "A"}
		u3 := &Song{Title: "A"}
		r.NoError(tx.Create(nil, u1))
		r.NoError(tx.Create(nil, u2))
		r.NoError(tx.Create(nil, u3))

		Debug = true
		defer func() { Debug = false }()

		var songs []Song
		err := tx.Where("id in (?)", u3.ID).Where("title = ?", "A").All(nil, &songs)
		r.NoError(err)
		r.Len(songs, 1)
	})
}

func Test_Where_In_Space(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	r := require.New(t)
	transaction(func(tx *Connection) {
		u1 := &Song{Title: "A"}
		u2 := &Song{Title: "B"}
		u3 := &Song{Title: "C"}
		r.NoError(tx.Create(nil, u1))
		r.NoError(tx.Create(nil, u2))
		r.NoError(tx.Create(nil, u3))

		Debug = true
		defer func() { Debug = false }()

		var songs []Song
		err := tx.Where("id in ( ? )", u1.ID, u3.ID).All(nil, &songs)
		r.NoError(err)
		r.Len(songs, 2)
	})
}

func Test_Order(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	a := require.New(t)

	m := NewModel(&Enemy{}, context.Background())
	q := PDB.Order("id desc")
	sql, _ := q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies ORDER BY id desc"), sql)

	q.Order("name desc")
	sql, _ = q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies ORDER BY id desc, name desc"), sql)
}

func Test_Order_With_Args(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	r := require.New(t)
	transaction(func(tx *Connection) {
		u1 := &Song{Title: "A"}
		u2 := &Song{Title: "B"}
		u3 := &Song{Title: "C"}
		r.NoError(tx.Create(nil, u1))
		r.NoError(tx.Create(nil, u2))
		r.NoError(tx.Create(nil, u3))

		var songs []Song
		err := tx.Where("id in (?)", []uuid.UUID{u1.ID, u2.ID, u3.ID}).
			Order("title > ? DESC", "A").Order("title").
			All(nil, &songs)
		r.NoError(err)
		r.Len(songs, 3)
		r.Equal("B", songs[0].Title)
		r.Equal("C", songs[1].Title)
		r.Equal("A", songs[2].Title)
	})
}

func Test_GroupBy(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	a := require.New(t)

	m := NewModel(&Enemy{}, context.Background())
	q := PDB.Q()
	q.GroupBy("A")
	sql, _ := q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies GROUP BY A"), sql)

	q = PDB.Q()
	q.GroupBy("A", "B")
	sql, _ = q.ToSQL(m)
	a.Equal(ts("SELECT enemies.A FROM enemies AS enemies GROUP BY A, B"), sql)

	q = PDB.Q()
	q.GroupBy("A", "B").Having("enemies.A=?", "test")
	sql, _ = q.ToSQL(m)
	if PDB.Dialect.Details().Dialect == "postgres" {
		a.Equal(ts("SELECT enemies.A FROM enemies AS enemies GROUP BY A, B HAVING enemies.A=$1"), sql)
	} else {
		a.Equal(ts("SELECT enemies.A FROM enemies AS enemies GROUP BY A, B HAVING enemies.A=?"), sql)
	}

	q = PDB.Q()
	q.GroupBy("A", "B").Having("enemies.A=?", "test").Having("enemies.B=enemies.A")
	sql, _ = q.ToSQL(m)
	if PDB.Dialect.Details().Dialect == "postgres" {
		a.Equal(ts("SELECT enemies.A FROM enemies AS enemies GROUP BY A, B HAVING enemies.A=$1 AND enemies.B=enemies.A"), sql)
	} else {
		a.Equal(ts("SELECT enemies.A FROM enemies AS enemies GROUP BY A, B HAVING enemies.A=? AND enemies.B=enemies.A"), sql)
	}
}

func Test_ToSQL(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	a := require.New(t)
	transaction(func(tx *Connection) {
		user := NewModel(&User{}, tx.Context())

		s := "SELECT name as full_name, users.alive, users.bio, users.birth_date, users.created_at, users.email, users.id, users.name, users.price, users.updated_at, users.user_name FROM users AS users"

		query := Q(tx)
		q, _ := query.ToSQL(user)
		a.Equal(s, q)

		query.Order("id desc")
		q, _ = query.ToSQL(user)
		a.Equal(fmt.Sprintf("%s ORDER BY id desc", s), q)

		q, _ = query.ToSQL(&Model{Value: &User{}, As: "u", ctx: tx.Context()})
		a.Equal("SELECT name as full_name, u.alive, u.bio, u.birth_date, u.created_at, u.email, u.id, u.name, u.price, u.updated_at, u.user_name FROM users AS u ORDER BY id desc", q)

		q, _ = query.ToSQL(&Model{Value: &Family{}, ctx: tx.Context()})
		a.Equal("SELECT family_members.created_at, family_members.first_name, family_members.id, family_members.last_name, family_members.updated_at FROM family.members AS family_members ORDER BY id desc", q)

		query = tx.Where("id = 1")
		q, _ = query.ToSQL(user)
		a.Equal(fmt.Sprintf("%s WHERE id = 1", s), q)

		query = tx.Where("id = 1").Where("name = 'Mark'")
		q, _ = query.ToSQL(user)
		a.Equal(fmt.Sprintf("%s WHERE id = 1 AND name = 'Mark'", s), q)

		query.Order("id desc")
		q, _ = query.ToSQL(user)
		a.Equal(fmt.Sprintf("%s WHERE id = 1 AND name = 'Mark' ORDER BY id desc", s), q)

		query.Order("name asc")
		q, _ = query.ToSQL(user)
		a.Equal(fmt.Sprintf("%s WHERE id = 1 AND name = 'Mark' ORDER BY id desc, name asc", s), q)

		query = tx.Limit(10)
		q, _ = query.ToSQL(user)
		a.Equal(fmt.Sprintf("%s LIMIT 10", s), q)

		query = tx.Paginate(3, 10)
		q, _ = query.ToSQL(user)
		a.Equal(fmt.Sprintf("%s LIMIT 10 OFFSET 20", s), q)

		// join must come first
		query = Q(tx).Where("id = ?", 1).Join("books b", "b.user_id=?", "xx").Order("name asc")
		q, args := query.ToSQL(user)

		if tx.Dialect.Details().Dialect == "postgres" {
			a.Equal(fmt.Sprintf("%s JOIN books b ON b.user_id=$1 WHERE id = $2 ORDER BY name asc", s), q)
		} else {
			a.Equal(fmt.Sprintf("%s JOIN books b ON b.user_id=? WHERE id = ? ORDER BY name asc", s), q)
		}
		// join arguments comes 1st
		a.Equal(args[0], "xx")
		a.Equal(args[1], 1)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct on (users.name, users.email) users.*", "users.bio")
		a.Equal("SELECT distinct on (users.name, users.email) users.*, users.bio FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct on (users.id) users.*", "users.bio")
		a.Equal("SELECT distinct on (users.id) users.*, users.bio FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "id,r", "users.bio,r", "users.email,w")
		a.Equal("SELECT id, users.bio FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct on (id) id,r", "users.bio,r", "email,w")
		a.Equal("SELECT distinct on (id) id, users.bio FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct id", "users.bio,r", "email,w")
		a.Equal("SELECT distinct id, users.bio FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct id", "concat(users.name,'-',users.email)")
		a.Equal("SELECT concat(users.name,'-',users.email), distinct id FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "id", "concat(users.name,'-',users.email) name_email")
		a.Equal("SELECT concat(users.name,'-',users.email) name_email, id FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct id", "concat(users.name,'-',users.email),r")
		a.Equal("SELECT concat(users.name,'-',users.email), distinct id FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct id", "concat(users.name,'-',users.email) AS x")
		a.Equal("SELECT concat(users.name,'-',users.email) AS x, distinct id FROM users AS users", q)

		query = Q(tx)
		q, _ = query.ToSQL(user, "distinct id", "users.name as english_name", "email private_email")
		a.Equal("SELECT distinct id, email private_email, users.name as english_name FROM users AS users", q)
	})
}

func Test_ToSQLInjection(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	a := require.New(t)
	transaction(func(tx *Connection) {
		user := NewModel(new(User), tx.Context())
		query := tx.Where("name = '?'", "\\\u0027 or 1=1 limit 1;\n-- ")
		q, _ := query.ToSQL(user)
		a.NotEqual("SELECT * FROM users AS users WHERE name = '\\'' or 1=1 limit 1;\n-- '", q)
	})
}

func Test_ToSQL_RawQuery(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	a := require.New(t)
	transaction(func(tx *Connection) {
		query := tx.RawQuery("this is some ? raw ?", "random", "query")
		q, args := query.ToSQL(nil)
		a.Equal(q, tx.Dialect.TranslateSQL("this is some ? raw ?"))
		a.Equal(args, []interface{}{"random", "query"})
	})
}

func Test_RawQuery_Empty(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	t.Run("EmptyQuery", func(t *testing.T) {
		r := require.New(t)
		transaction(func(tx *Connection) {
			r.Error(tx.Q().Exec(nil))
		})
	})

	t.Run("EmptyRawQuery", func(t *testing.T) {
		r := require.New(t)
		transaction(func(tx *Connection) {
			r.Error(tx.RawQuery("").Exec(nil))
		})
	})
}
