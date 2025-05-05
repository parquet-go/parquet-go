package parquet_test

import (
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestPrintSchema(t *testing.T) {
	tests := []struct {
		node  parquet.Node
		print string
	}{
		{
			node: parquet.Group{"on": parquet.Leaf(parquet.BooleanType)},
			print: `message Test {
	required boolean on;
}`,
		},
		{
			node: parquet.Group{"name": parquet.String()},
			print: `message Test {
	required binary name (STRING);
}`,
		},
		{
			node: parquet.Group{"uuid": parquet.UUID()},
			print: `message Test {
	required fixed_len_byte_array(16) uuid (UUID);
}`,
		},
		{
			node: parquet.Group{"enum": parquet.Enum()},
			print: `message Test {
	required binary enum (ENUM);
}`,
		},
		{
			node: parquet.Group{"json": parquet.JSON()},
			print: `message Test {
	required binary json (JSON);
}`,
		},
		{
			node: parquet.Group{"bson": parquet.BSON()},
			print: `message Test {
	required binary bson (BSON);
}`,
		},
		{
			node: parquet.Group{"name": parquet.Optional(parquet.String())},
			print: `message Test {
	optional binary name (STRING);
}`,
		},
		{
			node: parquet.Group{"name": parquet.Repeated(parquet.String())},
			print: `message Test {
	repeated binary name (STRING);
}`,
		},
		{
			node: parquet.Group{"age": parquet.Int(8)},
			print: `message Test {
	required int32 age (INT(8,true));
}`,
		},
		{
			node: parquet.Group{"age": parquet.Int(16)},
			print: `message Test {
	required int32 age (INT(16,true));
}`,
		},
		{
			node: parquet.Group{"age": parquet.Int(32)},
			print: `message Test {
	required int32 age (INT(32,true));
}`,
		},
		{
			node: parquet.Group{"age": parquet.Int(64)},
			print: `message Test {
	required int64 age (INT(64,true));
}`,
		},
		{
			node: parquet.Group{"age": parquet.Uint(8)},
			print: `message Test {
	required int32 age (INT(8,false));
}`,
		},
		{
			node: parquet.Group{"age": parquet.Uint(16)},
			print: `message Test {
	required int32 age (INT(16,false));
}`,
		},
		{
			node: parquet.Group{"age": parquet.Uint(32)},
			print: `message Test {
	required int32 age (INT(32,false));
}`,
		},
		{
			node: parquet.Group{"age": parquet.Uint(64)},
			print: `message Test {
	required int64 age (INT(64,false));
}`,
		},
		{
			node: parquet.Group{"ratio": parquet.Leaf(parquet.FloatType)},
			print: `message Test {
	required float ratio;
}`,
		},
		{
			node: parquet.Group{"ratio": parquet.Leaf(parquet.DoubleType)},
			print: `message Test {
	required double ratio;
}`,
		},
		{
			node: parquet.Group{"cost": parquet.Decimal(0, 9, parquet.Int32Type)},
			print: `message Test {
	required int32 cost (DECIMAL(9,0));
}`,
		},
		{
			node: parquet.Group{"cost": parquet.Decimal(0, 18, parquet.Int64Type)},
			print: `message Test {
	required int64 cost (DECIMAL(18,0));
}`,
		},
		{
			node: parquet.Group{"date": parquet.Date()},
			print: `message Test {
	required int32 date (DATE);
}`,
		},
		{
			node: parquet.Group{"time": parquet.Time(parquet.Millisecond)},
			print: `message Test {
	required int32 time (TIME(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},
		{
			node: parquet.Group{"time": parquet.Time(parquet.Microsecond)},
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=MICROS));
}`,
		},
		{
			node: parquet.Group{"time": parquet.Time(parquet.Nanosecond)},
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=NANOS));
}`,
		},
		{
			node: parquet.Group{"timestamp": parquet.Timestamp(parquet.Millisecond)},
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},
		{
			node: parquet.Group{"timestamp": parquet.Timestamp(parquet.Microsecond)},
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MICROS));
}`,
		},
		{
			node: parquet.Group{"timestamp": parquet.Timestamp(parquet.Nanosecond)},
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=NANOS));
}`,
		},
		{
			node: parquet.Group{"names": parquet.List(parquet.String())},
			print: `message Test {
	required group names (LIST) {
		repeated group list {
			required binary element (STRING);
		}
	}
}`,
		},
		{
			node: parquet.Group{
				"keys": parquet.List(
					parquet.Group{
						"key":   parquet.String(),
						"value": parquet.String(),
					},
				),
			},
			print: `message Test {
	required group keys (LIST) {
		repeated group list {
			required group element {
				required binary key (STRING);
				required binary value (STRING);
			}
		}
	}
}`,
		},
		{
			node: parquet.Group{
				"pairs": parquet.Map(
					parquet.String(),
					parquet.String(),
				),
			},
			print: `message Test {
	required group pairs (MAP) {
		repeated group key_value {
			required binary key (STRING);
			required binary value (STRING);
		}
	}
}`,
		},
		{
			node: parquet.GroupOfNodes("on", parquet.Leaf(parquet.BooleanType)),
			print: `message Test {
	required boolean on;
}`,
		},
		{
			node: parquet.GroupOfNodes("name", parquet.String()),
			print: `message Test {
	required binary name (STRING);
}`,
		},
		{
			node: parquet.GroupOfNodes("uuid", parquet.UUID()),
			print: `message Test {
	required fixed_len_byte_array(16) uuid (UUID);
}`,
		},
		{
			node: parquet.GroupOfNodes("enum", parquet.Enum()),
			print: `message Test {
	required binary enum (ENUM);
}`,
		},
		{
			node: parquet.GroupOfNodes("json", parquet.JSON()),
			print: `message Test {
	required binary json (JSON);
}`,
		},
		{
			node: parquet.GroupOfNodes("bson", parquet.BSON()),
			print: `message Test {
	required binary bson (BSON);
}`,
		},
		{
			node: parquet.GroupOfNodes("name", parquet.Optional(parquet.String())),
			print: `message Test {
	optional binary name (STRING);
}`,
		},
		{
			node: parquet.GroupOfNodes("name", parquet.Repeated(parquet.String())),
			print: `message Test {
	repeated binary name (STRING);
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Int(8)),
			print: `message Test {
	required int32 age (INT(8,true));
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Int(16)),
			print: `message Test {
	required int32 age (INT(16,true));
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Int(32)),
			print: `message Test {
	required int32 age (INT(32,true));
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Int(64)),
			print: `message Test {
	required int64 age (INT(64,true));
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Uint(8)),
			print: `message Test {
	required int32 age (INT(8,false));
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Uint(16)),
			print: `message Test {
	required int32 age (INT(16,false));
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Uint(32)),
			print: `message Test {
	required int32 age (INT(32,false));
}`,
		},
		{
			node: parquet.GroupOfNodes("age", parquet.Uint(64)),
			print: `message Test {
	required int64 age (INT(64,false));
}`,
		},
		{
			node: parquet.GroupOfNodes("ratio", parquet.Leaf(parquet.FloatType)),
			print: `message Test {
	required float ratio;
}`,
		},
		{
			node: parquet.GroupOfNodes("ratio", parquet.Leaf(parquet.DoubleType)),
			print: `message Test {
	required double ratio;
}`,
		},
		{
			node: parquet.GroupOfNodes("cost", parquet.Decimal(0, 9, parquet.Int32Type)),
			print: `message Test {
	required int32 cost (DECIMAL(9,0));
}`,
		},
		{
			node: parquet.GroupOfNodes("cost", parquet.Decimal(0, 18, parquet.Int64Type)),
			print: `message Test {
	required int64 cost (DECIMAL(18,0));
}`,
		},
		{
			node: parquet.GroupOfNodes("date", parquet.Date()),
			print: `message Test {
	required int32 date (DATE);
}`,
		},
		{
			node: parquet.GroupOfNodes("time", parquet.Time(parquet.Millisecond)),
			print: `message Test {
	required int32 time (TIME(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},
		{
			node: parquet.GroupOfNodes("time", parquet.Time(parquet.Microsecond)),
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=MICROS));
}`,
		},
		{
			node: parquet.GroupOfNodes("time", parquet.Time(parquet.Nanosecond)),
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=NANOS));
}`,
		},
		{
			node: parquet.GroupOfNodes("timestamp", parquet.Timestamp(parquet.Millisecond)),
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},
		{
			node: parquet.GroupOfNodes("timestamp", parquet.Timestamp(parquet.Microsecond)),
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MICROS));
}`,
		},
		{
			node: parquet.GroupOfNodes("timestamp", parquet.Timestamp(parquet.Nanosecond)),
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=NANOS));
}`,
		},
		{
			node: parquet.GroupOfNodes("names", parquet.List(parquet.String())),
			print: `message Test {
	required group names (LIST) {
		repeated group list {
			required binary element (STRING);
		}
	}
}`,
		},
		{
			node: parquet.GroupOfNodes(
				"keys", parquet.List(
					parquet.GroupOfNodes(
						"key", parquet.String(),
						"value", parquet.String(),
					),
				),
			),
			print: `message Test {
	required group keys (LIST) {
		repeated group list {
			required group element {
				required binary key (STRING);
				required binary value (STRING);
			}
		}
	}
}`,
		},
		{
			node: parquet.GroupOfNodes(
				"pairs", parquet.Map(
					parquet.String(),
					parquet.String(),
				),
			),
			print: `message Test {
	required group pairs (MAP) {
		repeated group key_value {
			required binary key (STRING);
			required binary value (STRING);
		}
	}
}`,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			buf := new(strings.Builder)

			if err := parquet.PrintSchema(buf, "Test", test.node); err != nil {
				t.Fatal(err)
			}

			if buf.String() != test.print {
				t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.print, buf)
			}
		})
	}
}
