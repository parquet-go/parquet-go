package parquet_test

import (
	"reflect"
	"testing"
	"time"

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
				Inner struct {
					TimeMillis int32         `parquet:"time_millis,time"`
					TimeMicros int64         `parquet:"time_micros,time(microsecond)"`
					TimeDur    time.Duration `parquet:"time_dur,time"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required int32 time_millis (TIME(isAdjustedToUTC=true,unit=MILLIS));
		required int64 time_micros (TIME(isAdjustedToUTC=true,unit=MICROS));
		required int64 time_dur (TIME(isAdjustedToUTC=true,unit=NANOS));
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
		{
			value: new(struct {
				Time time.Time `parquet:"time,delta"`
			}),
			print: `message {
	required int64 time (TIMESTAMP(isAdjustedToUTC=true,unit=NANOS));
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

func TestInvalidSchemaOf(t *testing.T) {
	tests := []struct {
		value interface{}
		panic string
	}{
		// Date tags must be int32
		{
			value: new(struct {
				Date float32 `parquet:",date"`
			}),
			panic: `date is an invalid parquet tag: Date float32 [date]`,
		},

		// Time tags must be int32 or int64. Additionally:
		// - int32 must be millisecond
		// - int64 must be microsecond or nanosecond
		// - Duration (int64) must be nanosecond
		// - If the unit is the problem, it is part of the message.
		{
			value: new(struct {
				Time float32 `parquet:",time"`
			}),
			panic: `time is an invalid parquet tag: Time float32 [time]`,
		},
		{
			value: new(struct {
				Time int32 `parquet:",time(microsecond)"`
			}),
			panic: `time(microsecond) is an invalid parquet tag: Time int32 [time(microsecond)]`,
		},
		{
			value: new(struct {
				Time int32 `parquet:",time(nanosecond)"`
			}),
			panic: `time(nanosecond) is an invalid parquet tag: Time int32 [time(nanosecond)]`,
		},
		{
			value: new(struct {
				Time int64 `parquet:",time(millisecond)"`
			}),
			panic: `time(millisecond) is an invalid parquet tag: Time int64 [time(millisecond)]`,
		},
		{
			value: new(struct {
				Time int64 `parquet:",time(notasecond)"`
			}),
			panic: `time(notasecond) is an invalid parquet tag: Time int64 [time(notasecond)]`,
		},
		{
			value: new(struct {
				Time time.Duration `parquet:",time(millisecond)"`
			}),
			panic: `time(millisecond) is an invalid parquet tag: Time time.Duration [time(millisecond)]`,
		},
		{
			value: new(struct {
				Time time.Duration `parquet:",time(microsecond)"`
			}),
			panic: `time(microsecond) is an invalid parquet tag: Time time.Duration [time(microsecond)]`,
		},

		// Timestamp tags must be int64 or time.Time
		// Additionally, if the unit is the problem, it is part of the message.
		{
			value: new(struct {
				Timestamp float32 `parquet:",timestamp"`
			}),
			panic: `timestamp is an invalid parquet tag: Timestamp float32 [timestamp]`,
		},
		{
			value: new(struct {
				Timestamp int32 `parquet:",timestamp"`
			}),
			panic: `timestamp is an invalid parquet tag: Timestamp int32 [timestamp]`,
		},
		{
			value: new(struct {
				Timestamp int32 `parquet:",timestamp(microsecond)"`
			}),
			panic: `timestamp is an invalid parquet tag: Timestamp int32 [timestamp]`,
		},
		{
			value: new(struct {
				Timestamp int64 `parquet:",timestamp(notasecond)"`
			}),
			panic: `timestamp(notasecond) is an invalid parquet tag: Timestamp int64 [timestamp(notasecond)]`,
		},
		{
			value: new(struct {
				Timestamp time.Time `parquet:",timestamp(notasecond)"`
			}),
			panic: `timestamp(notasecond) is an invalid parquet tag: Timestamp time.Time [timestamp(notasecond)]`,
		},
	}

	for _, test := range tests {
		ft := reflect.TypeOf(test.value).Elem().Field(0)
		t.Run(ft.Type.String()+" `"+ft.Tag.Get("parquet")+"`", func(t *testing.T) {
			defer func() {
				p := recover()
				if p == nil {
					t.Fatal("expected panic:", test.panic)
				}
				if p != test.panic {
					t.Fatalf("panic: got %q want %q", p, test.panic)
				}
			}()
			_ = parquet.SchemaOf(test.value)
		})
	}
}
