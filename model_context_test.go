package pop

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type ContextTable struct {
	ID        string    `db:"id"`
	Value     string    `db:"value"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (t ContextTable) TableName(ctx context.Context) string {
	// This is singular on purpose! It will check if the TableName is properly
	// Respected in slices as well.
	prefix := ctx.Value("prefix").(string)

	// PostgreSQL and CockroachDB support schemas which work like a prefix. For those cases, we use
	// the schema to ensure that name normalization does not cause query problems.
	//
	// Since this works only for those two databases, we use underscore for the rest.
	//
	// While this schema is hardcoded, it would have been too difficult to add this special
	// case to the migrations.
	switch PDB.Dialect.Name() {
	case nameCockroach:
		fallthrough
	case namePostgreSQL:
		prefix = prefix + "." + prefix
	}
	return "context_prefix_" + prefix + "_table"
}

func Test_ModelContext(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}

	t.Run("contextless", func(t *testing.T) {
		r := require.New(t)
		r.Panics(func() {
			var c ContextTable
			r.NoError(PDB.Create(nil, &c))
		}, "panics if context prefix is not set")
	})

	for _, prefix := range []string{"a", "b"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			r := require.New(t)

			expected := ContextTable{ID: prefix, Value: prefix}
			c := PDB.WithContext(context.WithValue(context.Background(), "prefix", prefix))
			r.NoError(c.Create(nil, &expected))

			var actual ContextTable
			r.NoError(c.Find(nil, &actual, expected.ID))
			r.EqualValues(prefix, actual.Value)
			r.EqualValues(prefix, actual.ID)

			exists, err := c.Where("id = ?", actual.ID).Exists(new(ContextTable))
			r.NoError(err)
			r.True(exists)

			count, err := c.Where("id = ?", actual.ID).Count(nil, new(ContextTable))
			r.NoError(err)
			r.EqualValues(1, count)

			expected.Value += expected.Value
			r.NoError(c.Update(nil, &expected))

			r.NoError(c.Find(nil, &actual, expected.ID))
			r.EqualValues(prefix+prefix, actual.Value)
			r.EqualValues(prefix, actual.ID)

			var results []ContextTable
			require.NoError(t, c.All(nil, &results))

			require.NoError(t, c.First(nil, &expected))
			require.NoError(t, c.Last(nil, &expected))

			r.NoError(c.Destroy(nil, &expected))
		})
	}

	t.Run("prefix=unknown", func(t *testing.T) {
		r := require.New(t)
		c := PDB.WithContext(context.WithValue(context.Background(), "prefix", "unknown"))
		err := c.Create(nil, &ContextTable{ID: "unknown"})
		r.Error(err)

		if !strings.Contains(err.Error(), "context_prefix_unknown") { // All other databases
			t.Fatalf("Expected error to contain indicator that table does not exist but got: %s", err.Error())
		}
	})

	t.Run("cache_busting", func(t *testing.T) {
		r := require.New(t)

		r.NoError(PDB.WithContext(context.WithValue(context.Background(), "prefix", "a")).Destroy(nil, &ContextTable{ID: "expectedA"}))
		r.NoError(PDB.WithContext(context.WithValue(context.Background(), "prefix", "b")).Destroy(nil, &ContextTable{ID: "expectedB"}))

		var expectedA, expectedB ContextTable
		expectedA.ID = "expectedA"
		expectedB.ID = "expectedB"

		cA := PDB.WithContext(context.WithValue(context.Background(), "prefix", "a"))
		r.NoError(cA.Create(nil, &expectedA))

		cB := PDB.WithContext(context.WithValue(context.Background(), "prefix", "b"))
		r.NoError(cB.Create(nil, &expectedB))

		var actualA, actualB []ContextTable
		r.NoError(cA.All(nil, &actualA))
		r.NoError(cB.All(nil, &actualB))

		r.Len(actualA, 1)
		r.Len(actualB, 1)

		r.NotEqual(actualA[0].ID, actualB[0].ID, "if these are equal context switching did not work")
	})
}
