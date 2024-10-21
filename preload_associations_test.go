package pop

import (
	"github.com/gobuffalo/nulls"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_New_Implementation_For_Nplus1(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)
		for _, name := range []string{"Mark", "Joe", "Jane"} {
			user := User{Name: nulls.NewString(name)}
			a.NoError(tx.Create(nil, &user))

			book := Book{UserID: nulls.NewInt(user.ID)}
			a.NoError(tx.Create(nil, &book))

			writer := Writer{Name: "Larry", BookID: book.ID}
			a.NoError(tx.Create(nil, &writer))

			if name == "Mark" {
				song := Song{UserID: user.ID}
				a.NoError(tx.Create(nil, &song))

				address := Address{Street: "Pop"}
				a.NoError(tx.Create(nil, &address))

				home := UsersAddress{UserID: user.ID, AddressID: address.ID}
				a.NoError(tx.Create(nil, &home))
			}
		}

		users := []User{}
		a.NoError(tx.All(nil, &users))

		// FILL THE HAS-MANY and HAS_ONE
		a.NoError(preload(tx, &users))

		a.Len(users[0].Books, 1)
		a.Len(users[1].Books, 1)
		a.Len(users[2].Books, 1)
		a.Equal(users[0].FavoriteSong.UserID, users[0].ID)
		a.Len(users[0].Houses, 1)

		book := Book{}
		a.NoError(tx.First(nil, &book))
		a.NoError(preload(tx, &book))
		a.Len(book.Writers, 1)
		a.Equal("Larry", book.Writers[0].Name)
		a.Equal("Mark", book.User.Name.String)

		usersWithPointers := []UserPointerAssocs{}
		a.NoError(tx.All(nil, &usersWithPointers))

		// FILL THE HAS-MANY and HAS_ONE
		a.NoError(preload(tx, &usersWithPointers))

		a.Len(usersWithPointers[0].Books, 1)
		a.Len(usersWithPointers[1].Books, 1)
		a.Len(usersWithPointers[2].Books, 1)
		a.Equal(usersWithPointers[0].FavoriteSong.UserID, users[0].ID)
		a.Len(usersWithPointers[0].Houses, 1)
	})
}

func Test_New_Implementation_For_Nplus1_With_UUID(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)

		courses := []Course{}
		for i := 0; i < 3; i++ {
			course := Course{}
			a.NoError(tx.Create(nil, &course))
			courses = append(courses, course)
			if i == 0 {
				a.NoError(tx.Create(nil, &CourseCode{
					CourseID: course.ID,
				}))
			}
		}

		courseCodes := []CourseCode{}
		a.NoError(tx.All(nil, &courseCodes))
		a.Len(courseCodes, 1)

		// FILL THE HAS-MANY and HAS_ONE
		a.NoError(preload(tx, &courseCodes))
		a.Equal(courses[0].ID, courseCodes[0].Course.ID)

		student := Student{}
		a.NoError(tx.Create(nil, &student))

		parent := Parent{}
		a.NoError(tx.Create(nil, &parent))

		a.NoError(tx.RawQuery("insert into parents_students(parent_id, student_id) values(?,?)", parent.ID.String(), student.ID.String()).Exec(nil))

		parents := []Parent{}
		a.NoError(tx.All(nil, &parents))

		a.NoError(preload(tx, &parents))
		a.Len(parents, 1)
		a.Len(parents[0].Students, 1)
		a.Equal(student.ID, parents[0].Students[0].ID)
	})
}

func Test_New_Implementation_For_Nplus1_With_NullUUIDs_And_FK_ID(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}

	// This test suite prevents regressions of an obscure bug in the preload code which caused
	// pointer values to be set with their empty values when relations did not exist.
	//
	// See also: https://github.com/gobuffalo/pop/issues/139
	transaction(func(tx *Connection) {
		a := require.New(t)

		var server Server
		a.NoError(tx.Create(nil, &server))

		class := &NetClient{
			// The bug only appears when we have two elements in the slice where
			// one has a relation and the other one has no such relation.
			Hops: []Hop{
				{Server: &server},
				{},
			}}

		// This code basically just sets up
		a.NoError(tx.Eager().Create(nil, class))

		var expected NetClient
		a.NoError(tx.EagerPreload("Hops.Server").First(nil, &expected))

		// What would happen before the patch resolved this issue is that:
		//
		// Classes.CourseCodes[0].Course would be the correct value (a filled struct)
		//
		//   "server": {
		//     "id": "fa51f71f-e884-4641-8005-923258b814f9",
		//     "created_at": "2021-12-09T23:20:10.208019+01:00",
		//     "updated_at": "2021-12-09T23:20:10.208019+01:00"
		//   },
		//
		// Classes.CourseCodes[1].Course would an "empty" struct of Course even though there is no relation set up:
		//
		//	  "server": {
		//      "id": "00000000-0000-0000-0000-000000000000",
		//      "created_at": "0001-01-01T00:00:00Z",
		//      "updated_at": "0001-01-01T00:00:00Z"
		//    },
		var foundValid, foundEmpty int
		for _, hop := range expected.Hops {
			if hop.ServerID.Valid {
				foundValid++
				a.NotNil(hop.Server, "%+v", hop)
			} else {
				foundEmpty++
				a.Nil(hop.Server, "%+v", hop)
			}
		}

		a.Equal(1, foundValid)
		a.Equal(1, foundEmpty)
	})
}

func Test_New_Implementation_For_Nplus1_Single(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)
		for _, name := range []string{"Mark", "Joe", "Jane"} {
			user := User{Name: nulls.NewString(name)}
			a.NoError(tx.Create(nil, &user))

			book := Book{UserID: nulls.NewInt(user.ID)}
			a.NoError(tx.Create(nil, &book))

			writer := Writer{Name: "Larry", BookID: book.ID}
			a.NoError(tx.Create(nil, &writer))

			if name == "Mark" {
				song := Song{UserID: user.ID}
				a.NoError(tx.Create(nil, &song))

				address := Address{Street: "Pop"}
				a.NoError(tx.Create(nil, &address))

				home := UsersAddress{UserID: user.ID, AddressID: address.ID}
				a.NoError(tx.Create(nil, &home))
			}
		}

		users := []User{}
		a.NoError(tx.All(nil, &users))

		// FILL THE HAS-MANY and HAS_ONE
		a.NoError(preload(tx, &users, "Books"))

		a.Len(users[0].Books, 1)
		a.Len(users[1].Books, 1)
		a.Len(users[2].Books, 1)
		a.Zero(users[0].FavoriteSong.UserID)
		a.Len(users[0].Houses, 0)
	})
}

func Test_New_Implementation_For_Nplus1_Nested(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)
		var song Song
		for _, name := range []string{"Mark", "Joe", "Jane"} {
			user := User{Name: nulls.NewString(name)}
			a.NoError(tx.Create(nil, &user))

			book := Book{UserID: nulls.NewInt(user.ID)}
			a.NoError(tx.Create(nil, &book))

			if name == "Mark" {
				song = Song{UserID: user.ID}
				a.NoError(tx.Create(nil, &song))

				address := Address{Street: "Pop"}
				a.NoError(tx.Create(nil, &address))

				home := UsersAddress{UserID: user.ID, AddressID: address.ID}
				a.NoError(tx.Create(nil, &home))
			}
		}

		SetEagerMode(EagerPreload)
		users := []User{}
		a.NoError(tx.Eager("Houses", "Books", "Books.User.FavoriteSong").All(nil, &users))
		a.Len(users[0].Books, 1)
		a.Len(users[1].Books, 1)
		a.Len(users[2].Books, 1)
		a.Len(users[0].Houses, 1)

		a.Equal(users[0].ID, users[0].Books[0].User.ID)
		a.Equal(song.ID, users[0].Books[0].User.FavoriteSong.ID)
		SetEagerMode(EagerDefault)
	})
}

func Test_New_Implementation_For_Nplus1_BelongsTo_Not_Underscore(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)
		user := User{Name: nulls.NewString("Mark")}
		a.NoError(tx.Create(nil, &user))

		taxi := Taxi{UserID: nulls.NewInt(user.ID)}
		a.NoError(tx.Create(nil, &taxi))

		SetEagerMode(EagerPreload)
		taxis := []Taxi{}
		a.NoError(tx.EagerPreload().All(nil, &taxis))
		a.Len(taxis, 1)
		a.Equal("Mark", taxis[0].Driver.Name.String)
		SetEagerMode(EagerDefault)
	})
}

func Test_New_Implementation_For_BelongsTo_Multiple_Fields(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)
		user := User{Name: nulls.NewString("Mark")}
		a.NoError(tx.Create(nil, &user))

		address := Address{HouseNumber: 2, Street: "Street One"}
		a.NoError(tx.Create(nil, &address))

		taxi := Taxi{UserID: nulls.NewInt(user.ID), AddressID: nulls.NewInt(address.ID)}
		a.NoError(tx.Create(nil, &taxi))

		book := Book{TaxiID: nulls.NewInt(taxi.ID), Title: "My Book"}
		a.NoError(tx.Create(nil, &book))

		SetEagerMode(EagerPreload)
		books := []Book{}
		a.NoError(tx.EagerPreload("Taxi.Driver", "Taxi.Address").All(nil, &books))
		a.Len(books, 1)
		a.Equal(user.Name.String, books[0].Taxi.Driver.Name.String)
		a.Equal(address.Street, books[0].Taxi.Address.Street)
		SetEagerMode(EagerDefault)
	})
}

func Test_New_Implementation_For_BelongsTo_Ptr_Field(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)
		toAddress := Address{HouseNumber: 1, Street: "Destination Ave"}
		a.NoError(tx.Create(nil, &toAddress))

		taxi := Taxi{ToAddressID: &toAddress.ID}
		a.NoError(tx.Create(nil, &taxi))

		book1 := Book{TaxiID: nulls.NewInt(taxi.ID), Title: "My Book"}
		a.NoError(tx.Create(nil, &book1))

		taxiNilToAddress := Taxi{ToAddressID: nil}
		a.NoError(tx.Create(nil, &taxiNilToAddress))

		book2 := Book{TaxiID: nulls.NewInt(taxiNilToAddress.ID), Title: "Another Book"}
		a.NoError(tx.Create(nil, &book2))

		SetEagerMode(EagerPreload)
		books := []Book{}
		a.NoError(tx.EagerPreload("Taxi.ToAddress").Order("created_at").All(nil, &books))
		a.Len(books, 2)
		a.Equal(toAddress.Street, books[0].Taxi.ToAddress.Street)
		a.NotNil(books[0].Taxi.ToAddressID)
		a.Nil(books[1].Taxi.ToAddress)
		a.Nil(books[1].Taxi.ToAddressID)
		SetEagerMode(EagerDefault)
	})
}

func Test_New_Implementation_For_HasMany_Ptr_Field(t *testing.T) {
	if PDB == nil {
		t.Skip("skipping integration tests")
	}
	transaction(func(tx *Connection) {
		a := require.New(t)
		toAddress1 := Address{HouseNumber: 1, Street: "Destination Ave"}
		a.NoError(tx.Create(nil, &toAddress1))
		taxi1 := Taxi{Model: "Ford", ToAddressID: &toAddress1.ID}
		a.NoError(tx.Create(nil, &taxi1))
		taxi2 := Taxi{Model: "Honda", ToAddressID: &toAddress1.ID}
		a.NoError(tx.Create(nil, &taxi2))

		taxiNilToAddress := Taxi{ToAddressID: nil}
		a.NoError(tx.Create(nil, &taxiNilToAddress))

		toAddress2 := Address{HouseNumber: 2, Street: "Final Way"}
		a.NoError(tx.Create(nil, &toAddress2))
		taxi3 := Taxi{Model: "Mazda", ToAddressID: &toAddress2.ID}
		a.NoError(tx.Create(nil, &taxi3))

		SetEagerMode(EagerPreload)
		addresses := []Address{}
		a.NoError(tx.EagerPreload("TaxisToHere").Order("created_at").All(nil, &addresses))
		a.Len(addresses, 2)
		a.NotNil(addresses[0].TaxisToHere)
		a.Len(addresses[0].TaxisToHere, 2)
		a.Equal(taxi1.Model, addresses[0].TaxisToHere[0].Model)
		a.Equal(taxi2.Model, addresses[0].TaxisToHere[1].Model)
		a.NotNil(addresses[1].TaxisToHere)
		a.Len(addresses[1].TaxisToHere, 1)
		a.Equal(taxi3.Model, addresses[1].TaxisToHere[0].Model)
		SetEagerMode(EagerDefault)
	})
}
