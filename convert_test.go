package parquet_test

import (
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

type AddressBook1 struct {
	Owner             string   `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string `parquet:"ownerPhoneNumbers,gzip"`
}

type AddressBook2 struct {
	Owner             string    `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers,gzip"`
	Contacts          []Contact `parquet:"contacts"`
	Extra             string    `parquet:"extra"`
}

type AddressBook3 struct {
	Owner    string     `parquet:"owner,zstd"`
	Contacts []Contact2 `parquet:"contacts"`
}

type Contact2 struct {
	Name         string   `parquet:"name"`
	PhoneNumbers []string `parquet:"phoneNumbers,zstd"`
	Addresses    []string `parquet:"addresses,zstd"`
}

type AddressBook4 struct {
	Owner    string     `parquet:"owner,zstd"`
	Contacts []Contact2 `parquet:"contacts"`
	Extra    string     `parquet:"extra"`
}

type SimpleNumber struct {
	Number *int64 `parquet:"number,optional"`
}

type SimpleContact struct {
	Numbers []SimpleNumber `parquet:"numbers"`
}

type SimpleAddressBook struct {
	Name    string
	Contact SimpleContact
}

type SimpleAddressBook2 struct {
	Name    string
	Contact SimpleContact
	Extra   string
}

type ListOfIDs struct {
	IDs []uint64
}

var conversionTests = [...]struct {
	scenario string
	from     any
	to       any
}{
	{
		scenario: "convert between rows which have the same schema",
		from: AddressBook{
			Owner: "Julien Le Dem",
			OwnerPhoneNumbers: []string{
				"555 123 4567",
				"555 666 1337",
			},
			Contacts: []Contact{
				{
					Name:        "Dmitriy Ryaboy",
					PhoneNumber: "555 987 6543",
				},
				{
					Name: "Chris Aniszczyk",
				},
			},
		},
		to: AddressBook{
			Owner: "Julien Le Dem",
			OwnerPhoneNumbers: []string{
				"555 123 4567",
				"555 666 1337",
			},
			Contacts: []Contact{
				{
					Name:        "Dmitriy Ryaboy",
					PhoneNumber: "555 987 6543",
				},
				{
					Name: "Chris Aniszczyk",
				},
			},
		},
	},

	{
		scenario: "missing column",
		from:     struct{ FirstName, LastName string }{FirstName: "Luke", LastName: "Skywalker"},
		to:       struct{ LastName string }{LastName: "Skywalker"},
	},

	{
		scenario: "missing optional column",
		from: struct {
			FirstName *string
			LastName  string
		}{FirstName: newString("Luke"), LastName: "Skywalker"},
		to: struct{ LastName string }{LastName: "Skywalker"},
	},

	{
		scenario: "missing repeated column",
		from: struct {
			ID    uint64
			Names []string
		}{ID: 42, Names: []string{"me", "myself", "I"}},
		to: struct{ ID uint64 }{ID: 42},
	},

	{
		scenario: "extra column",
		from:     struct{ LastName string }{LastName: "Skywalker"},
		to:       struct{ FirstName, LastName string }{LastName: "Skywalker"},
	},

	{
		scenario: "extra optional column",
		from:     struct{ ID uint64 }{ID: 2},
		to: struct {
			ID      uint64
			Details *struct{ FirstName, LastName string }
		}{ID: 2, Details: nil},
	},

	{
		scenario: "extra repeated column",
		from:     struct{ ID uint64 }{ID: 1},
		to: struct {
			ID    uint64
			Names []string
		}{ID: 1, Names: []string{}},
	},

	{
		scenario: "extra required column from repeated",
		from: struct{ ListOfIDs ListOfIDs }{
			ListOfIDs: ListOfIDs{IDs: []uint64{0, 1, 2}},
		},
		to: struct {
			MainID    uint64
			ListOfIDs ListOfIDs
		}{
			ListOfIDs: ListOfIDs{IDs: []uint64{0, 1, 2}},
		},
	},

	{
		scenario: "extra fields in repeated group",
		from: struct{ Books []AddressBook1 }{
			Books: []AddressBook1{
				{
					Owner:             "me",
					OwnerPhoneNumbers: []string{"123-456-7890", "321-654-0987"},
				},
				{
					Owner:             "you",
					OwnerPhoneNumbers: []string{"000-000-0000"},
				},
			},
		},
		to: struct{ Books []AddressBook2 }{
			Books: []AddressBook2{
				{
					Owner:             "me",
					OwnerPhoneNumbers: []string{"123-456-7890", "321-654-0987"},
					Contacts:          []Contact{},
				},
				{
					Owner:             "you",
					OwnerPhoneNumbers: []string{"000-000-0000"},
					Contacts:          []Contact{},
				},
			},
		},
	},

	{
		scenario: "extra column on complex struct",
		from: AddressBook{
			Owner:             "Julien Le Dem",
			OwnerPhoneNumbers: []string{},
			Contacts: []Contact{
				{
					Name:        "Dmitriy Ryaboy",
					PhoneNumber: "555 987 6543",
				},
				{
					Name: "Chris Aniszczyk",
				},
			},
		},
		to: AddressBook2{
			Owner:             "Julien Le Dem",
			OwnerPhoneNumbers: []string{},
			Contacts: []Contact{
				{
					Name:        "Dmitriy Ryaboy",
					PhoneNumber: "555 987 6543",
				},
				{
					Name: "Chris Aniszczyk",
				},
			},
		},
	},

	{
		scenario: "required to optional leaf",
		from:     struct{ Name string }{Name: "Luke"},
		to:       struct{ Name *string }{Name: newString("Luke")},
	},

	{
		scenario: "required to repeated leaf",
		from:     struct{ Name string }{Name: "Luke"},
		to:       struct{ Name []string }{Name: []string{"Luke"}},
	},

	{
		scenario: "optional to required leaf",
		from:     struct{ Name *string }{Name: newString("Luke")},
		to:       struct{ Name string }{Name: "Luke"},
	},

	{
		scenario: "optional to repeated leaf",
		from:     struct{ Name *string }{Name: newString("Luke")},
		to:       struct{ Name []string }{Name: []string{"Luke"}},
	},

	{
		scenario: "optional to repeated leaf (null)",
		from:     struct{ Name *string }{Name: nil},
		to:       struct{ Name []string }{Name: []string{}},
	},

	{
		scenario: "repeated to required leaf",
		from:     struct{ Name []string }{Name: []string{"Luke", "Han", "Leia"}},
		to:       struct{ Name string }{Name: "Luke"},
	},

	{
		scenario: "repeated to optional leaf",
		from:     struct{ Name []string }{Name: []string{"Luke", "Han", "Leia"}},
		to:       struct{ Name *string }{Name: newString("Luke")},
	},

	{
		scenario: "required to optional group",
		from: struct{ Book AddressBook }{
			Book: AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
		},
		to: struct{ Book *AddressBook }{
			Book: &AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
		},
	},

	{
		scenario: "required to optional group (empty)",
		from: struct{ Book AddressBook }{
			Book: AddressBook{},
		},
		to: struct{ Book *AddressBook }{
			Book: &AddressBook{
				OwnerPhoneNumbers: []string{},
				Contacts:          []Contact{},
			},
		},
	},

	{
		scenario: "optional to required group (null)",
		from: struct{ Book *AddressBook }{
			Book: nil,
		},
		to: struct{ Book AddressBook }{
			Book: AddressBook{
				OwnerPhoneNumbers: []string{},
				Contacts:          []Contact{},
			},
		},
	},

	{
		scenario: "optional to repeated group (null)",
		from:     struct{ Book *AddressBook }{Book: nil},
		to:       struct{ Book []AddressBook }{Book: []AddressBook{}},
	},

	{
		scenario: "optional to repeated optional group (null)",
		from:     struct{ Book *AddressBook }{Book: nil},
		to:       struct{ Book []*AddressBook }{Book: []*AddressBook{}},
	},

	{
		scenario: "handle nested repeated elements during conversion",
		from: AddressBook3{
			Owner: "Julien Le Dem",
			Contacts: []Contact2{
				{
					Name: "Dmitriy Ryaboy",
					PhoneNumbers: []string{
						"555 987 6543",
						"555 123 4567",
					},
					Addresses: []string{},
				},
				{
					Name: "Chris Aniszczyk",
					PhoneNumbers: []string{
						"555 345 8129",
					},
					Addresses: []string{
						"42 Wallaby Way Sydney",
						"1 White House Way",
					},
				},
				{
					Name: "Bob Ross",
					PhoneNumbers: []string{
						"555 198 3628",
					},
					Addresses: []string{
						"::1",
					},
				},
			},
		},
		to: AddressBook4{
			Owner: "Julien Le Dem",
			Contacts: []Contact2{
				{
					Name: "Dmitriy Ryaboy",
					PhoneNumbers: []string{
						"555 987 6543",
						"555 123 4567",
					},
					Addresses: []string{},
				},
				{
					Name: "Chris Aniszczyk",
					PhoneNumbers: []string{
						"555 345 8129",
					},
					Addresses: []string{
						"42 Wallaby Way Sydney",
						"1 White House Way",
					},
				},
				{
					Name: "Bob Ross",
					PhoneNumbers: []string{
						"555 198 3628",
					},
					Addresses: []string{
						"::1",
					},
				},
			},
			Extra: "",
		},
	},

	{
		scenario: "handle nested repeated elements during conversion",
		from: SimpleAddressBook{
			Name: "New Contact",
			Contact: SimpleContact{
				Numbers: []SimpleNumber{
					{
						Number: nil,
					},
					{
						Number: newInt64(1329),
					},
				},
			},
		},
		to: SimpleAddressBook2{
			Name: "New Contact",
			Contact: SimpleContact{
				Numbers: []SimpleNumber{
					{
						Number: nil,
					},
					{
						Number: newInt64(1329),
					},
				},
			},
			Extra: "",
		},
	},
}

func TestConvert(t *testing.T) {
	for _, test := range conversionTests {
		t.Run(test.scenario, func(t *testing.T) {
			to := parquet.SchemaOf(test.to)
			from := parquet.SchemaOf(test.from)

			conv, err := parquet.Convert(to, from)
			if err != nil {
				t.Fatal(err)
			}

			row := from.Deconstruct(nil, test.from)
			rowbuf := []parquet.Row{row}
			n, err := conv.Convert(rowbuf)
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("wrong number of rows got converted: want=1 got=%d", n)
			}
			row = rowbuf[0]

			value := reflect.New(reflect.TypeOf(test.to))
			if err := to.Reconstruct(value.Interface(), row); err != nil {
				t.Fatal(err)
			}

			value = value.Elem()
			if !reflect.DeepEqual(value.Interface(), test.to) {
				t.Errorf("converted value mismatch:\nwant = %#v\ngot  = %#v", test.to, value.Interface())
			}
		})
	}
}

func newInt64(i int64) *int64    { return &i }
func newString(s string) *string { return &s }

func TestConvertValue(t *testing.T) {
	now := time.Unix(42, 0)
	ms := now.UnixMilli()
	us := now.UnixMicro()
	ns := now.UnixNano()

	msType := parquet.Timestamp(parquet.Millisecond).Type()
	msVal := parquet.ValueOf(ms)
	if msVal.Int64() != ms {
		t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", ms, msVal.Int64())
	}

	usType := parquet.Timestamp(parquet.Microsecond).Type()
	usVal := parquet.ValueOf(us)
	if usVal.Int64() != us {
		t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", us, usVal.Int64())
	}

	nsType := parquet.Timestamp(parquet.Nanosecond).Type()
	nsVal := parquet.ValueOf(ns)
	if nsVal.Int64() != ns {
		t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", ns, nsVal.Int64())
	}

	var timestampConversionTests = [...]struct {
		scenario  string
		fromType  parquet.Type
		fromValue parquet.Value
		toType    parquet.Type
		toValue   parquet.Value
	}{
		{
			scenario:  "true to boolean",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(true),
		},

		{
			scenario:  "true to int32",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.Int32Type,
			toValue:   parquet.Int32Value(1),
		},

		{
			scenario:  "true to int64",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(1),
		},

		{
			scenario:  "true to int96",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.Int96Type,
			toValue:   parquet.Int96Value(deprecated.Int96{0: 1}),
		},

		{
			scenario:  "true to float",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(1),
		},

		{
			scenario:  "true to double",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(1),
		},

		{
			scenario:  "true to byte array",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte{1}),
		},

		{
			scenario:  "true to fixed length byte array",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.FixedLenByteArrayType(4),
			toValue:   parquet.FixedLenByteArrayValue([]byte{1, 0, 0, 0}),
		},

		{
			scenario:  "true to string",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(true),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`true`)),
		},

		{
			scenario:  "false to boolean",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(false),
		},

		{
			scenario:  "false to int32",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.Int32Type,
			toValue:   parquet.Int32Value(0),
		},

		{
			scenario:  "false to int64",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(0),
		},

		{
			scenario:  "false to int96",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.Int96Type,
			toValue:   parquet.Int96Value(deprecated.Int96{}),
		},

		{
			scenario:  "false to float",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(0),
		},

		{
			scenario:  "false to double",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(0),
		},

		{
			scenario:  "false to byte array",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte{0}),
		},

		{
			scenario:  "false to fixed length byte array",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.FixedLenByteArrayType(4),
			toValue:   parquet.FixedLenByteArrayValue([]byte{0, 0, 0, 0}),
		},

		{
			scenario:  "false to string",
			fromType:  parquet.BooleanType,
			fromValue: parquet.BooleanValue(false),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`false`)),
		},

		{
			scenario:  "int32 to true",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(10),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(true),
		},

		{
			scenario:  "int32 to false",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(0),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(false),
		},

		{
			scenario:  "int32 to int32",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(42),
			toType:    parquet.Int32Type,
			toValue:   parquet.Int32Value(42),
		},

		{
			scenario:  "int32 to int64",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(-21),
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(-21),
		},

		{
			scenario:  "int32 to int96",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(123),
			toType:    parquet.Int96Type,
			toValue:   parquet.Int96Value(deprecated.Int96{0: 123}),
		},

		{
			scenario:  "int32 to float",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(9),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(9),
		},

		{
			scenario:  "int32 to double",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(100),
			toType:    parquet.DoubleType,
			toValue:   parquet.DoubleValue(100),
		},

		{
			scenario:  "int32 to byte array",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(1 << 8),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte{0, 1, 0, 0}),
		},

		{
			scenario:  "int32 to fixed length byte array",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(1 << 8),
			toType:    parquet.FixedLenByteArrayType(3),
			toValue:   parquet.FixedLenByteArrayValue([]byte{0, 1, 0}),
		},

		{
			scenario:  "int32 to string",
			fromType:  parquet.Int32Type,
			fromValue: parquet.Int32Value(12345),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`12345`)),
		},

		{
			scenario:  "int64 to true",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(10),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(true),
		},

		{
			scenario:  "int64 to false",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(0),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(false),
		},

		{
			scenario:  "int64 to int32",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(-21),
			toType:    parquet.Int32Type,
			toValue:   parquet.Int32Value(-21),
		},

		{
			scenario:  "int64 to int64",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(42),
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(42),
		},

		{
			scenario:  "int64 to int96",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(123),
			toType:    parquet.Int96Type,
			toValue:   parquet.Int96Value(deprecated.Int96{0: 123}),
		},

		{
			scenario:  "int64 to float",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(9),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(9),
		},

		{
			scenario:  "int64 to double",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(100),
			toType:    parquet.DoubleType,
			toValue:   parquet.DoubleValue(100),
		},

		{
			scenario:  "int64 to byte array",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(1 << 8),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte{0, 1, 0, 0, 0, 0, 0, 0}),
		},

		{
			scenario:  "int64 to fixed length byte array",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(1 << 8),
			toType:    parquet.FixedLenByteArrayType(3),
			toValue:   parquet.FixedLenByteArrayValue([]byte{0, 1, 0}),
		},

		{
			scenario:  "int64 to string",
			fromType:  parquet.Int64Type,
			fromValue: parquet.Int64Value(1234567890),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`1234567890`)),
		},

		{
			scenario:  "float to true",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(0.1),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(true),
		},

		{
			scenario:  "float to false",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(0),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(false),
		},

		{
			scenario:  "float to int32",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(9.9),
			toType:    parquet.Int32Type,
			toValue:   parquet.Int32Value(9),
		},

		{
			scenario:  "float to int64",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(-1.5),
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(-1),
		},

		{
			scenario:  "float to float",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(1.234),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(1.234),
		},

		{
			scenario:  "float to double",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(-0.5),
			toType:    parquet.DoubleType,
			toValue:   parquet.DoubleValue(-0.5),
		},

		{
			scenario:  "float to string",
			fromType:  parquet.FloatType,
			fromValue: parquet.FloatValue(0.125),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`0.125`)),
		},

		{
			scenario:  "double to true",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(0.1),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(true),
		},

		{
			scenario:  "double to false",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(0),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(false),
		},

		{
			scenario:  "double to int32",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(9.9),
			toType:    parquet.Int32Type,
			toValue:   parquet.Int32Value(9),
		},

		{
			scenario:  "double to int64",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(-1.5),
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(-1),
		},

		{
			scenario:  "double to float",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(1.234),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(1.234),
		},

		{
			scenario:  "double to double",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(-0.5),
			toType:    parquet.DoubleType,
			toValue:   parquet.DoubleValue(-0.5),
		},

		{
			scenario:  "double to string",
			fromType:  parquet.DoubleType,
			fromValue: parquet.DoubleValue(0.125),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`0.125`)),
		},

		{
			scenario:  "string to true",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`true`)),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(true),
		},

		{
			scenario:  "string to false",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`false`)),
			toType:    parquet.BooleanType,
			toValue:   parquet.BooleanValue(false),
		},

		{
			scenario:  "string to int32",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`-21`)),
			toType:    parquet.Int32Type,
			toValue:   parquet.Int32Value(-21),
		},

		{
			scenario:  "string to int64",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`42`)),
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(42),
		},

		{
			scenario:  "string to int96",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`123`)),
			toType:    parquet.Int96Type,
			toValue:   parquet.Int96Value(deprecated.Int96{0: 123}),
		},

		{
			scenario:  "string to float",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`-0.5`)),
			toType:    parquet.FloatType,
			toValue:   parquet.FloatValue(-0.5),
		},

		{
			scenario:  "string to double",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`0.5`)),
			toType:    parquet.DoubleType,
			toValue:   parquet.DoubleValue(0.5),
		},

		{
			scenario:  "string to byte array",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`ABC`)),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte(`ABC`)),
		},

		{
			scenario:  "string to fixed length byte array",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`99B816772522447EBF76821A7C5ADF65`)),
			toType:    parquet.FixedLenByteArrayType(16),
			toValue: parquet.FixedLenByteArrayValue([]byte{
				0x99, 0xb8, 0x16, 0x77, 0x25, 0x22, 0x44, 0x7e,
				0xbf, 0x76, 0x82, 0x1a, 0x7c, 0x5a, 0xdf, 0x65,
			}),
		},

		{
			scenario:  "string to string",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`Hello World!`)),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`Hello World!`)),
		},

		{
			scenario:  "string to date",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`1970-01-03`)),
			toType:    parquet.Date().Type(),
			toValue:   parquet.Int32Value(2),
		},

		{
			scenario:  "string to millisecond time",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`12:34:56.789`)),
			toType:    parquet.Time(parquet.Millisecond).Type(),
			toValue:   parquet.Int32Value(45296789),
		},

		{
			scenario:  "string to microsecond time",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`12:34:56.789012`)),
			toType:    parquet.Time(parquet.Microsecond).Type(),
			toValue:   parquet.Int64Value(45296789012),
		},

		{
			scenario:  "date to millisecond timestamp",
			fromType:  parquet.Date().Type(),
			fromValue: parquet.Int32Value(19338),
			toType:    parquet.Timestamp(parquet.Millisecond).Type(),
			toValue:   parquet.Int64Value(1670803200000),
		},

		{
			scenario:  "date to microsecond timestamp",
			fromType:  parquet.Date().Type(),
			fromValue: parquet.Int32Value(19338),
			toType:    parquet.Timestamp(parquet.Microsecond).Type(),
			toValue:   parquet.Int64Value(1670803200000000),
		},

		{
			scenario:  "date to string",
			fromType:  parquet.Date().Type(),
			fromValue: parquet.Int32Value(18995),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`2022-01-03`)),
		},

		{
			scenario:  "millisecond time to string",
			fromType:  parquet.Time(parquet.Millisecond).Type(),
			fromValue: parquet.Int32Value(45296789),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`12:34:56.789`)),
		},

		{
			scenario:  "microsecond time to string",
			fromType:  parquet.Time(parquet.Microsecond).Type(),
			fromValue: parquet.Int64Value(45296789012),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`12:34:56.789012`)),
		},

		{
			scenario:  "millisecond timestamp to date",
			fromType:  parquet.Timestamp(parquet.Millisecond).Type(),
			fromValue: parquet.Int64Value(1670888613000),
			toType:    parquet.Date().Type(),
			toValue:   parquet.Int32Value(19338),
		},

		{
			scenario:  "microsecond timestamp to date",
			fromType:  parquet.Timestamp(parquet.Microsecond).Type(),
			fromValue: parquet.Int64Value(1670888613000123),
			toType:    parquet.Date().Type(),
			toValue:   parquet.Int32Value(19338),
		},

		{
			scenario:  "millisecond timestamp to millisecond time",
			fromType:  parquet.Timestamp(parquet.Millisecond).Type(),
			fromValue: parquet.Int64Value(1670888613123),
			toType:    parquet.Time(parquet.Millisecond).Type(),
			toValue:   parquet.Int32Value(85413123),
		},

		{
			scenario:  "millisecond timestamp to micronsecond time",
			fromType:  parquet.Timestamp(parquet.Millisecond).Type(),
			fromValue: parquet.Int64Value(1670888613123),
			toType:    parquet.Time(parquet.Microsecond).Type(),
			toValue:   parquet.Int64Value(85413123000),
		},

		{
			scenario:  "microsecond timestamp to millisecond time",
			fromType:  parquet.Timestamp(parquet.Microsecond).Type(),
			fromValue: parquet.Int64Value(1670888613123456),
			toType:    parquet.Time(parquet.Millisecond).Type(),
			toValue:   parquet.Int32Value(85413123),
		},

		{
			scenario:  "microsecond timestamp to micronsecond time",
			fromType:  parquet.Timestamp(parquet.Microsecond).Type(),
			fromValue: parquet.Int64Value(1670888613123456),
			toType:    parquet.Time(parquet.Microsecond).Type(),
			toValue:   parquet.Int64Value(85413123456),
		},

		{
			scenario:  "micros to nanos",
			fromType:  usType,
			fromValue: usVal,
			toType:    nsType,
			toValue:   parquet.Int64Value(ns),
		},

		{
			scenario:  "millis to nanos",
			fromType:  msType,
			fromValue: msVal,
			toType:    nsType,
			toValue:   parquet.Int64Value(ns),
		},

		{
			scenario:  "nanos to micros",
			fromType:  nsType,
			fromValue: nsVal,
			toType:    usType,
			toValue:   parquet.Int64Value(us),
		},

		{
			scenario:  "nanos to nanos",
			fromType:  nsType,
			fromValue: nsVal,
			toType:    nsType,
			toValue:   parquet.Int64Value(ns),
		},

		{
			scenario:  "int64 to nanos",
			fromType:  parquet.Int64Type,
			fromValue: nsVal,
			toType:    nsType,
			toValue:   parquet.Int64Value(ns),
		},

		{
			scenario:  "int64 to int64",
			fromType:  parquet.Int64Type,
			fromValue: nsVal,
			toType:    parquet.Int64Type,
			toValue:   parquet.Int64Value(ns),
		},

		// JSON and BYTE_ARRAY conversions - regression test for type assertion bug
		// where jsonType.ConvertValue used *byteArrayType (pointer) but byteArrayType
		// is a value type, causing BYTE_ARRAY to JSON conversion to fail.
		{
			scenario:  "byte array to json",
			fromType:  parquet.ByteArrayType,
			fromValue: parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
			toType:    parquet.JSON().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
		},
		{
			scenario:  "json to byte array",
			fromType:  parquet.JSON().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
		},
		{
			scenario:  "string to json",
			fromType:  parquet.String().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
			toType:    parquet.JSON().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
		},
		{
			scenario:  "json to string",
			fromType:  parquet.JSON().Type(),
			fromValue: parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
			toType:    parquet.String().Type(),
			toValue:   parquet.ByteArrayValue([]byte(`{"key":"value"}`)),
		},

		// BSON conversions - same fix as JSON
		{
			scenario:  "byte array to bson",
			fromType:  parquet.ByteArrayType,
			fromValue: parquet.ByteArrayValue([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
			toType:    parquet.BSON().Type(),
			toValue:   parquet.ByteArrayValue([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
		},
		{
			scenario:  "bson to byte array",
			fromType:  parquet.BSON().Type(),
			fromValue: parquet.ByteArrayValue([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
		},

		// ENUM conversions - same fix as JSON
		{
			scenario:  "byte array to enum",
			fromType:  parquet.ByteArrayType,
			fromValue: parquet.ByteArrayValue([]byte("VALUE")),
			toType:    parquet.Enum().Type(),
			toValue:   parquet.ByteArrayValue([]byte("VALUE")),
		},
		{
			scenario:  "enum to byte array",
			fromType:  parquet.Enum().Type(),
			fromValue: parquet.ByteArrayValue([]byte("VALUE")),
			toType:    parquet.ByteArrayType,
			toValue:   parquet.ByteArrayValue([]byte("VALUE")),
		},
	}

	for _, test := range timestampConversionTests {
		t.Run(test.scenario, func(t *testing.T) {
			// Set levels to ensure that they are retained by the conversion.
			from := test.fromValue.Level(1, 2, 3)
			want := test.toValue.Level(1, 2, 3)

			got, err := test.toType.ConvertValue(from, test.fromType)
			if err != nil {
				t.Fatal(err)
			}

			if !parquet.DeepEqual(want, got) {
				t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", want, got)
			}
		})
	}
}

func TestMissingColumnChunk(t *testing.T) {
	type stringRow struct{ StringVal string }
	schema := parquet.SchemaOf(&stringRow{})
	buffer := parquet.NewGenericBuffer[stringRow](schema)
	if _, err := buffer.Write([]stringRow{{"hello"}, {"world"}}); err != nil {
		t.Fatal(err)
	}

	type boolRow struct{ BoolValue bool }
	conv := convertMissingColumn{
		schema: parquet.SchemaOf(&boolRow{}),
	}
	boolRowGroup := parquet.ConvertRowGroup(buffer, conv)
	chunk := boolRowGroup.ColumnChunks()[0]

	t.Run("chunk values", func(t *testing.T) {
		if chunk.NumValues() != buffer.NumRows() {
			t.Fatal("chunk values mismatch, got", chunk.NumValues(), "want", buffer.NumRows())
		}
	})

	t.Run("slice page", func(t *testing.T) {
		page, err := chunk.Pages().ReadPage()
		if err != nil {
			t.Fatal(err)
		}

		if page.NumValues() != buffer.NumRows() {
			t.Fatalf("page size mismatch: want = %d, got = %d", buffer.NumRows(), page.NumValues())
		}
		if size := page.Slice(0, 1).NumValues(); size != 1 {
			t.Fatalf("page slice size mismatch: want = %d, got = %d", 1, size)
		}
	})
}

type convertMissingColumn struct {
	schema *parquet.Schema
}

func (m convertMissingColumn) Column(_ int) int                        { return -1 }
func (m convertMissingColumn) Schema() *parquet.Schema                 { return m.schema }
func (m convertMissingColumn) Convert(rows []parquet.Row) (int, error) { return len(rows), nil }

func TestConvertRowGroupColumnIndexes(t *testing.T) {
	type OriginalSchema struct {
		A int64  `parquet:"a"`
		B string `parquet:"b"`
		C bool   `parquet:"c"`
	}

	type TargetSchema struct {
		A int64  `parquet:"a"`
		C bool   `parquet:"c"`
		B string `parquet:"b"`
	}

	originalSchema := parquet.SchemaOf(&OriginalSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	buffer := parquet.NewGenericBuffer[OriginalSchema](originalSchema)
	if _, err := buffer.Write([]OriginalSchema{
		{A: 1, B: "hello", C: true},
		{A: 2, B: "world", C: false},
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, originalSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	chunks := convertedRowGroup.ColumnChunks()

	if len(chunks) != 3 {
		t.Fatalf("expected 3 column chunks, got %d", len(chunks))
	}

	expectedColumns := []struct {
		name          string
		columnIndex   int
		originalIndex int
	}{
		{"a", 0, 0},
		{"b", 1, 1},
		{"c", 2, 2},
	}

	for _, expected := range expectedColumns {
		chunk := chunks[expected.columnIndex]
		gotColumnIndex := chunk.Column()

		if gotColumnIndex != expected.columnIndex {
			t.Errorf("column %q at position %d: Column() returned %d, want %d",
				expected.name, expected.columnIndex, gotColumnIndex, expected.columnIndex)
		}
	}
}

func TestConvertRowGroupValueColumnIndexes(t *testing.T) {
	type OriginalSchema struct {
		ID       int64   `parquet:"id"`
		Required string  `parquet:"required"`
		Optional *string `parquet:"optional,optional"`
	}

	type TargetSchema struct {
		ID       int64   `parquet:"id"`
		Optional *string `parquet:"optional,optional"`
		Required string  `parquet:"required"`
	}

	originalSchema := parquet.SchemaOf(&OriginalSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	optionalValue := "test"
	buffer := parquet.NewGenericBuffer[OriginalSchema](originalSchema)
	if _, err := buffer.Write([]OriginalSchema{
		{ID: 1, Required: "value1", Optional: &optionalValue},
		{ID: 2, Required: "value2", Optional: nil},
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, originalSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	chunks := convertedRowGroup.ColumnChunks()

	if len(chunks) != 3 {
		t.Fatalf("expected 3 column chunks, got %d", len(chunks))
	}

	for chunkIndex, chunk := range chunks {
		pages := chunk.Pages()
		defer pages.Close()

		for {
			page, err := pages.ReadPage()
			if err != nil {
				break
			}

			reader := page.Values()
			values := make([]parquet.Value, 1024)

			for {
				n, err := reader.ReadValues(values)
				for i := range n {
					gotColumnIndex := values[i].Column()
					if gotColumnIndex != chunkIndex {
						t.Errorf("chunk %d: value has columnIndex %d, want %d",
							chunkIndex, gotColumnIndex, chunkIndex)
					}
				}

				if err != nil {
					break
				}
			}
		}
	}
}

func TestConvertRowGroupWithMissingColumns(t *testing.T) {
	type OriginalSchema struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	type TargetSchema struct {
		ID    int64   `parquet:"id"`
		Extra *string `parquet:"extra,optional"`
		Name  string  `parquet:"name"`
	}

	originalSchema := parquet.SchemaOf(&OriginalSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	buffer := parquet.NewGenericBuffer[OriginalSchema](originalSchema)
	if _, err := buffer.Write([]OriginalSchema{
		{ID: 1, Name: "first"},
		{ID: 2, Name: "second"},
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, originalSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	chunks := convertedRowGroup.ColumnChunks()

	if len(chunks) != 3 {
		t.Fatalf("expected 3 column chunks, got %d", len(chunks))
	}

	for chunkIndex, chunk := range chunks {
		gotColumnIndex := chunk.Column()
		if gotColumnIndex != chunkIndex {
			t.Errorf("chunk %d: Column() returned %d, want %d",
				chunkIndex, gotColumnIndex, chunkIndex)
		}

		pages := chunk.Pages()
		defer pages.Close()

		for {
			page, err := pages.ReadPage()
			if err != nil {
				break
			}

			reader := page.Values()
			values := make([]parquet.Value, 1024)

			for {
				n, err := reader.ReadValues(values)
				for i := range n {
					gotValueColumnIndex := values[i].Column()
					if gotValueColumnIndex != chunkIndex {
						t.Errorf("chunk %d: value has columnIndex %d, want %d",
							chunkIndex, gotValueColumnIndex, chunkIndex)
					}
				}

				if err != nil {
					break
				}
			}
		}
	}
}

// TestConvertMissingRequiredColumnDirect tests direct page reading
func TestConvertMissingRequiredColumnDirect(t *testing.T) {
	type SourceSchema struct {
		ID int64 `parquet:"id"`
	}

	type TargetSchema struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	sourceSchema := parquet.SchemaOf(&SourceSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	buffer := parquet.NewGenericBuffer[SourceSchema](sourceSchema)
	if _, err := buffer.Write([]SourceSchema{{ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	chunks := convertedRowGroup.ColumnChunks()

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}

	// Log all chunks
	for i, chunk := range chunks {
		t.Logf("Chunk %d: column=%d, type=%v", i, chunk.Column(), chunk.Type())
	}

	// Read from the Name column directly (index 1)
	nameChunk := chunks[1]
	t.Logf("Name chunk type: %v, kind: %v", nameChunk.Type(), nameChunk.Type().Kind())

	pages := nameChunk.Pages()
	defer pages.Close()

	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal("error reading page:", err)
	}

	reader := page.Values()
	values := make([]parquet.Value, 10)
	n, err := reader.ReadValues(values)
	t.Logf("Read %d values from missing column, err=%v", n, err)
	for i := range n {
		t.Logf("  value[%d]: col=%d, isNull=%v, kind=%v, bytes=%q", i, values[i].Column(), values[i].IsNull(), values[i].Kind(), values[i].ByteArray())
	}
}

// TestConvertMissingRequiredColumn tests that missing required columns produce zero/default values
func TestConvertMissingRequiredColumn(t *testing.T) {
	type SourceSchema struct {
		ID int64 `parquet:"id"`
	}

	type TargetSchema struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"` // Missing required column should get empty string
	}

	sourceSchema := parquet.SchemaOf(&SourceSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	buffer := parquet.NewGenericBuffer[SourceSchema](sourceSchema)
	if _, err := buffer.Write([]SourceSchema{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	rows := convertedRowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]parquet.Row, 1)
	expectedNames := []string{"", "", ""} // Empty strings for missing required column
	expectedIDs := []int64{1, 2, 3}

	for i := range 3 {
		n, err := rows.ReadRows(rowBuf)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected 1 row, got %d", n)
		}

		row := rowBuf[0]
		var result TargetSchema
		if err := targetSchema.Reconstruct(&result, row); err != nil {
			t.Fatal(err)
		}

		expected := TargetSchema{ID: expectedIDs[i], Name: expectedNames[i]}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("row %d: expected %+v, got %+v", i, expected, result)
		}

		// Check values directly for levels
		nameColumnIndex := 1 // "name" is second column
		var foundNameValue bool
		for _, val := range row {
			if val.Column() == nameColumnIndex {
				foundNameValue = true
				// Required field should have definitionLevel == maxDefinitionLevel
				// For a required field at root, maxDefinitionLevel should be 0
				// But if it's part of the schema, check it produces a zero value
				if val.IsNull() {
					t.Errorf("row %d: missing required field should not be null", i)
				}
				// Value should be empty string (zero value for string)
				if val.String() != "" {
					t.Errorf("row %d: expected empty string, got %q", i, val.String())
				}
			}
		}
		if !foundNameValue {
			t.Errorf("row %d: did not find value for name column", i)
		}
	}
}

// TestConvertMissingOptionalColumn tests that missing optional columns produce nulls
func TestConvertMissingOptionalColumn(t *testing.T) {
	type SourceSchema struct {
		ID int64 `parquet:"id"`
	}

	type TargetSchema struct {
		ID      int64   `parquet:"id"`
		Comment *string `parquet:"comment,optional"` // Missing optional column should be null
	}

	sourceSchema := parquet.SchemaOf(&SourceSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	buffer := parquet.NewGenericBuffer[SourceSchema](sourceSchema)
	if _, err := buffer.Write([]SourceSchema{
		{ID: 1},
		{ID: 2},
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	rows := convertedRowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]parquet.Row, 1)

	for i := range 2 {
		n, err := rows.ReadRows(rowBuf)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected 1 row, got %d", n)
		}

		row := rowBuf[0]
		var result TargetSchema
		if err := targetSchema.Reconstruct(&result, row); err != nil {
			t.Fatal(err)
		}

		expected := TargetSchema{ID: int64(i + 1), Comment: nil}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("row %d: expected %+v, got %+v", i, expected, result)
		}

		// Check values directly for levels
		commentColumnIndex := 1 // "comment" is second column
		for _, val := range row {
			if val.Column() == commentColumnIndex {
				// Optional field should have null value
				// definitionLevel < maxDefinitionLevel indicates null
				if !val.IsNull() {
					t.Errorf("row %d: missing optional field should be null", i)
				}
			}
		}
	}
}

// TestConvertMissingRequiredInRepeatedGroup tests missing required column in a repeated group
func TestConvertMissingRequiredInRepeatedGroup(t *testing.T) {
	type Item struct {
		X int32 `parquet:"x"`
	}

	type ItemWithY struct {
		X int32 `parquet:"x"`
		Y int32 `parquet:"y"` // Missing required field should get zero value
	}

	type SourceSchema struct {
		Items []Item `parquet:"items"`
	}

	type TargetSchema struct {
		Items []ItemWithY `parquet:"items"`
	}

	sourceSchema := parquet.SchemaOf(&SourceSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	buffer := parquet.NewGenericBuffer[SourceSchema](sourceSchema)
	if _, err := buffer.Write([]SourceSchema{
		{Items: []Item{{X: 1}, {X: 2}, {X: 3}}}, // 3 items in first row
		{Items: []Item{{X: 4}}},                 // 1 item in second row
		{Items: []Item{}},                       // 0 items in third row
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	rows := convertedRowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]parquet.Row, 1)

	// Row 0: 3 items
	n, err := rows.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}
	var result TargetSchema
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}
	expectedRow0 := TargetSchema{
		Items: []ItemWithY{{X: 1, Y: 0}, {X: 2, Y: 0}, {X: 3, Y: 0}},
	}
	if !reflect.DeepEqual(result, expectedRow0) {
		t.Errorf("row 0: expected %+v, got %+v", expectedRow0, result)
	}

	// Verify repetition and definition levels match between X and Y columns
	row := rowBuf[0]
	xValues := []parquet.Value{}
	yValues := []parquet.Value{}
	for _, val := range row {
		if val.Column() == 0 { // X column
			xValues = append(xValues, val)
		} else if val.Column() == 1 { // Y column
			yValues = append(yValues, val)
		}
	}
	if len(xValues) != len(yValues) {
		t.Errorf("row 0: X column has %d values, Y column has %d values", len(xValues), len(yValues))
	}
	for i := range xValues {
		if xValues[i].RepetitionLevel() != yValues[i].RepetitionLevel() {
			t.Errorf("row 0, value %d: X repLevel=%d, Y repLevel=%d (should match)",
				i, xValues[i].RepetitionLevel(), yValues[i].RepetitionLevel())
		}
		if xValues[i].DefinitionLevel() != yValues[i].DefinitionLevel() {
			t.Errorf("row 0, value %d: X defLevel=%d, Y defLevel=%d (should match for required fields)",
				i, xValues[i].DefinitionLevel(), yValues[i].DefinitionLevel())
		}
	}

	// Row 1: 1 item
	n, err = rows.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}
	expectedRow1 := TargetSchema{
		Items: []ItemWithY{{X: 4, Y: 0}},
	}
	if !reflect.DeepEqual(result, expectedRow1) {
		t.Errorf("row 1: expected %+v, got %+v", expectedRow1, result)
	}

	// Row 2: 0 items
	n, err = rows.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}
	expectedRow2 := TargetSchema{
		Items: []ItemWithY{},
	}
	if !reflect.DeepEqual(result, expectedRow2) {
		t.Errorf("row 2: expected %+v, got %+v", expectedRow2, result)
	}
}

// TestConvertMissingOptionalInRepeatedGroup tests missing optional column in a repeated group
func TestConvertMissingOptionalInRepeatedGroup(t *testing.T) {
	type Item struct {
		X int32 `parquet:"x"`
	}

	type ItemWithY struct {
		X int32  `parquet:"x"`
		Y *int32 `parquet:"y,optional"` // Missing optional field should be null
	}

	type SourceSchema struct {
		Items []Item `parquet:"items"`
	}

	type TargetSchema struct {
		Items []ItemWithY `parquet:"items"`
	}

	sourceSchema := parquet.SchemaOf(&SourceSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	buffer := parquet.NewGenericBuffer[SourceSchema](sourceSchema)
	if _, err := buffer.Write([]SourceSchema{
		{Items: []Item{{X: 1}, {X: 2}}},
		{Items: []Item{{X: 3}}},
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	rows := convertedRowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]parquet.Row, 1)

	// Row 0: 2 items
	n, err := rows.ReadRows(rowBuf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}
	var result TargetSchema
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}
	expectedRow0 := TargetSchema{
		Items: []ItemWithY{{X: 1, Y: nil}, {X: 2, Y: nil}},
	}
	if !reflect.DeepEqual(result, expectedRow0) {
		t.Errorf("row 0: expected %+v, got %+v", expectedRow0, result)
	}

	// Verify repetition levels match, but definition levels indicate NULL
	row := rowBuf[0]
	xValues := []parquet.Value{}
	yValues := []parquet.Value{}
	for _, val := range row {
		if val.Column() == 0 { // X column
			xValues = append(xValues, val)
		} else if val.Column() == 1 { // Y column
			yValues = append(yValues, val)
		}
	}
	if len(xValues) != len(yValues) {
		t.Errorf("row 0: X column has %d values, Y column has %d values", len(xValues), len(yValues))
	}
	for i := range xValues {
		if xValues[i].RepetitionLevel() != yValues[i].RepetitionLevel() {
			t.Errorf("row 0, value %d: X repLevel=%d, Y repLevel=%d (should match)",
				i, xValues[i].RepetitionLevel(), yValues[i].RepetitionLevel())
		}
		// For optional fields, Y should be NULL (isNull=true)
		if !yValues[i].IsNull() {
			t.Errorf("row 0, value %d: Y value should be NULL", i)
		}
	}
}

// TestConvertMissingOptionalInRepeatedGroupWithOptionalAdjacent tests missing optional column
// where the adjacent column is also optional
func TestConvertMissingOptionalInRepeatedGroupWithOptionalAdjacent(t *testing.T) {
	type Item struct {
		X *int32 `parquet:"x,optional"`
	}

	type ItemWithY struct {
		X *int32 `parquet:"x,optional"`
		Y *int32 `parquet:"y,optional"` // Missing optional field should be null
	}

	type SourceSchema struct {
		Items []Item `parquet:"items"`
	}

	type TargetSchema struct {
		Items []ItemWithY `parquet:"items"`
	}

	sourceSchema := parquet.SchemaOf(&SourceSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	// Create test data with a mix of present and null values for X
	x1 := int32(1)
	x3 := int32(3)
	buffer := parquet.NewGenericBuffer[SourceSchema](sourceSchema)
	if _, err := buffer.Write([]SourceSchema{
		{Items: []Item{{X: &x1}, {X: nil}, {X: &x3}}}, // Row 0: 3 items (some with null X)
		{Items: []Item{{X: nil}}},                     // Row 1: 1 item with null X
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	rows := convertedRowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]parquet.Row, 1)

	// Row 0: 3 items
	n, err := rows.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}
	var result TargetSchema
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}

	expectedRow0 := TargetSchema{
		Items: []ItemWithY{{X: &x1, Y: nil}, {X: nil, Y: nil}, {X: &x3, Y: nil}},
	}
	if !reflect.DeepEqual(result, expectedRow0) {
		t.Errorf("row 0: expected %+v, got %+v", expectedRow0, result)
	}

	// Verify repetition levels match between X and Y
	row := rowBuf[0]
	xValues := []parquet.Value{}
	yValues := []parquet.Value{}
	for _, val := range row {
		if val.Column() == 0 { // X column
			xValues = append(xValues, val)
		} else if val.Column() == 1 { // Y column
			yValues = append(yValues, val)
		}
	}
	if len(xValues) != len(yValues) {
		t.Errorf("row 0: X column has %d values, Y column has %d values", len(xValues), len(yValues))
	}
	for i := range xValues {
		if xValues[i].RepetitionLevel() != yValues[i].RepetitionLevel() {
			t.Errorf("row 0, value %d: X repLevel=%d, Y repLevel=%d (should match)",
				i, xValues[i].RepetitionLevel(), yValues[i].RepetitionLevel())
		}
		// Y should always be NULL
		if !yValues[i].IsNull() {
			t.Errorf("row 0, value %d: Y value should be NULL", i)
		}
		// When X is null, Y should also be null at the same level
		// When X is present, Y should be null at the leaf level
		if xValues[i].IsNull() {
			// Both should be null, definition levels should match
			if xValues[i].DefinitionLevel() != yValues[i].DefinitionLevel() {
				t.Errorf("row 0, value %d: when X is null, defLevels should match: X=%d, Y=%d",
					i, xValues[i].DefinitionLevel(), yValues[i].DefinitionLevel())
			}
		}
	}

	// Row 1: 1 item with null X
	n, err = rows.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}
	expectedRow1 := TargetSchema{
		Items: []ItemWithY{{X: nil, Y: nil}},
	}
	if !reflect.DeepEqual(result, expectedRow1) {
		t.Errorf("row 1: expected %+v, got %+v", expectedRow1, result)
	}
}

// TestConvertMissingRequiredInRepeatedGroupWithOptionalAdjacent tests missing required column
// where the adjacent column is optional
func TestConvertMissingRequiredInRepeatedGroupWithOptionalAdjacent(t *testing.T) {
	type Item struct {
		X *int32 `parquet:"x,optional"`
	}

	type ItemWithY struct {
		X *int32 `parquet:"x,optional"`
		Y int32  `parquet:"y"` // Missing required field should get zero value
	}

	type SourceSchema struct {
		Items []Item `parquet:"items"`
	}

	type TargetSchema struct {
		Items []ItemWithY `parquet:"items"`
	}

	sourceSchema := parquet.SchemaOf(&SourceSchema{})
	targetSchema := parquet.SchemaOf(&TargetSchema{})

	// Create test data with a mix of present and null values for X
	x1 := int32(1)
	x3 := int32(3)
	buffer := parquet.NewGenericBuffer[SourceSchema](sourceSchema)
	if _, err := buffer.Write([]SourceSchema{
		{Items: []Item{{X: &x1}, {X: nil}, {X: &x3}}}, // Row 0: 3 items (some with null X)
		{Items: []Item{{X: nil}}},                     // Row 1: 1 item with null X
	}); err != nil {
		t.Fatal(err)
	}

	conversion, err := parquet.Convert(targetSchema, sourceSchema)
	if err != nil {
		t.Fatal(err)
	}

	convertedRowGroup := parquet.ConvertRowGroup(buffer, conversion)
	rows := convertedRowGroup.Rows()
	defer rows.Close()

	rowBuf := make([]parquet.Row, 1)

	// Row 0: 3 items
	n, err := rows.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}
	var result TargetSchema
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}
	expectedRow0 := TargetSchema{
		Items: []ItemWithY{{X: &x1, Y: 0}, {X: nil, Y: 0}, {X: &x3, Y: 0}},
	}
	if !reflect.DeepEqual(result, expectedRow0) {
		t.Errorf("row 0: expected %+v, got %+v", expectedRow0, result)
	}

	// Verify repetition levels match between X and Y
	row := rowBuf[0]
	xValues := []parquet.Value{}
	yValues := []parquet.Value{}
	for _, val := range row {
		if val.Column() == 0 { // X column
			xValues = append(xValues, val)
		} else if val.Column() == 1 { // Y column
			yValues = append(yValues, val)
		}
	}
	if len(xValues) != len(yValues) {
		t.Errorf("row 0: X column has %d values, Y column has %d values", len(xValues), len(yValues))
	}
	for i := range xValues {
		if xValues[i].RepetitionLevel() != yValues[i].RepetitionLevel() {
			t.Errorf("row 0, value %d: X repLevel=%d, Y repLevel=%d (should match)",
				i, xValues[i].RepetitionLevel(), yValues[i].RepetitionLevel())
		}
		// Y is required, so should always be non-null
		if yValues[i].IsNull() {
			t.Errorf("row 0, value %d: Y should not be NULL (required field)", i)
		}
		// Y should have value 0
		if yValues[i].Int32() != 0 {
			t.Errorf("row 0, value %d: expected Y=0, got %d", i, yValues[i].Int32())
		}
	}

	// Row 1: 1 item with null X
	n, err = rows.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if err := targetSchema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatal(err)
	}
	expectedRow1 := TargetSchema{
		Items: []ItemWithY{{X: nil, Y: 0}},
	}
	if !reflect.DeepEqual(result, expectedRow1) {
		t.Errorf("row 1: expected %+v, got %+v", expectedRow1, result)
	}
}

func BenchmarkConvertLargeSchemaDifferent(b *testing.B) {
	const numColumns = 30000
	const numCommonColumns = 25000 // 25k columns in common, 5k different

	// Build two schemas with many columns, but with some differences
	toFields := make(parquet.Group)
	fromFields := make(parquet.Group)

	for i := range numColumns {
		var toFieldName, fromFieldName string
		var fieldNode parquet.Node

		if i < numCommonColumns {
			// Common columns
			toFieldName = fmt.Sprintf("field_%d", i)
			fromFieldName = toFieldName
		} else {
			// Different columns for to vs from
			toFieldName = fmt.Sprintf("to_field_%d", i)
			fromFieldName = fmt.Sprintf("from_field_%d", i)
		}

		// Alternate between different types
		switch i % 5 {
		case 0:
			fieldNode = parquet.Int(64)
		case 1:
			fieldNode = parquet.String()
		case 2:
			fieldNode = parquet.Int(32)
		case 3:
			fieldNode = parquet.Leaf(parquet.DoubleType)
		case 4:
			fieldNode = parquet.Leaf(parquet.BooleanType)
		}

		toFields[toFieldName] = fieldNode
		fromFields[fromFieldName] = fieldNode
	}

	toSchema := parquet.NewSchema("benchmark", toFields)
	fromSchema := parquet.NewSchema("benchmark", fromFields)

	b.ResetTimer()
	for b.Loop() {
		_, err := parquet.Convert(toSchema, fromSchema)
		if err != nil {
			b.Fatal(err)
		}
	}
}
