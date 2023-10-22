package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestSchemaOf(t *testing.T) {
	tests := []struct {
		value interface{}
		print string
	}{
		{
			value: new(struct{ Name string }),
			print: `message {
	required binary Name (STRING);
}`,
		},

		{
			value: new(struct {
				X int
				Y int
			}),
			print: `message {
	required int64 X (INT(64,true));
	required int64 Y (INT(64,true));
}`,
		},

		{
			value: new(struct {
				X float32
				Y float32
			}),
			print: `message {
	required float X;
	required float Y;
}`,
		},

		{
			value: new(struct {
				Inner struct {
					FirstName string `parquet:"first_name"`
					LastName  string `parquet:"last_name"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required binary first_name (STRING);
		required binary last_name (STRING);
	}
}`,
		},

		{
			value: new(struct {
				Short float32 `parquet:"short,split"`
				Long  float64 `parquet:"long,split"`
			}),
			print: `message {
	required float short;
	required double long;
}`,
		},

		{
			value: new(struct {
				Inner struct {
					FirstName          string `parquet:"first_name"`
					ShouldNotBePresent string `parquet:"-"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required binary first_name (STRING);
	}
}`,
		},

		{
			value: new(struct {
				Inner struct {
					FirstName    string `parquet:"first_name"`
					MyNameIsDash string `parquet:"-,"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required binary first_name (STRING);
		required binary - (STRING);
	}
}`,
		},

		{
			value: new(struct {
				Inner struct {
					TimestampMillis int64 `parquet:"timestamp_millis,timestamp"`
					TimestampMicros int64 `parquet:"timestamp_micros,timestamp(microsecond)"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required int64 timestamp_millis (TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS));
		required int64 timestamp_micros (TIMESTAMP(isAdjustedToUTC=true,unit=MICROS));
	}
}`,
		},

		{
			value: new(struct {
				Name string `parquet:",json"`
			}),
			print: `message {
	required binary Name (JSON);
}`,
		},

		{
			value: new(struct {
				A map[int64]string `parquet:"," parquet-key:",timestamp"`
				B map[int64]string
			}),
			print: `message {
	required group A (MAP) {
		repeated group key_value {
			required int64 key (TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS));
			required binary value (STRING);
		}
	}
	required group B (MAP) {
		repeated group key_value {
			required int64 key (INT(64,true));
			required binary value (STRING);
		}
	}
}`,
		},

		{
			value: new(struct {
				A map[int64]string `parquet:",optional" parquet-value:",json"`
			}),
			print: `message {
	optional group A (MAP) {
		repeated group key_value {
			required int64 key (INT(64,true));
			required binary value (JSON);
		}
	}
}`,
		},

		{
			value: new(struct {
				A map[int64]string `parquet:",optional"`
			}),
			print: `message {
	optional group A (MAP) {
		repeated group key_value {
			required int64 key (INT(64,true));
			required binary value (STRING);
		}
	}
}`,
		},

		{
			value: new(struct {
				A map[int64]string `parquet:",optional" parquet-value:",json" parquet-key:",timestamp(microsecond)"`
			}),
			print: `message {
	optional group A (MAP) {
		repeated group key_value {
			required int64 key (TIMESTAMP(isAdjustedToUTC=true,unit=MICROS));
			required binary value (JSON);
		}
	}
}`,
		},
		{
			value: new(struct {
				A struct {
					B string `parquet:"b,id(2)"`
				} `parquet:"a,id(1)"`
				C map[string]string `parquet:"c,id(3)"`
				D []string          `parquet:"d,id(4)"`
				E string            `parquet:"e,optional,id(5)"`
			}),
			print: `message {
	required group a = 1 {
		required binary b (STRING) = 2;
	}
	required group c (MAP) = 3 {
		repeated group key_value {
			required binary key (STRING);
			required binary value (STRING);
		}
	}
	repeated binary d (STRING) = 4;
	optional binary e (STRING) = 5;
}`,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			schema := parquet.SchemaOf(test.value)

			if s := schema.String(); s != test.print {
				t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.print, s)
			}
		})
	}
}
