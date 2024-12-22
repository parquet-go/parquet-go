package main

import (
	"bufio"
	"log"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
)

func main() {
	input := make([]Record, 998_227)

	for i := range input {
		input[i] = Record{
			Uint1:    uint32(i + 0),
			Uint2:    uint32(i + 1),
			String1:  "string1",
			String2:  "string2",
			String3:  "string3",
			String4:  "string4",
			String5:  "string5",
			String6:  "string6",
			String7:  "string7",
			String8:  "string8",
			String9:  "string9",
			String10: "string10",
			String11: "string11",
			String12: "string12",
			String13: "string13",
			Bool1:    (i % 2) == 0,
			Int1:     int(i + 3),
			Int2:     int(i + 4),
			Int3:     int(i + 5),
			Int4:     int(i + 6),
			Int5:     int(i + 7),
			Int6:     int(i + 8),
			Int7:     int(i + 9),
			String14: "string14",
			String15: "string15",
			Bool2:    (i % 3) == 0,
			Bool3:    (i % 4) == 0,
			Bool4:    (i % 5) == 0,
			Time1:    time.Now(),
			Float1:   1.0 + float64(i),
			Float2:   2.0 + float64(i),
			Uint3:    uint32(i + 10),
			Float3:   3.0 + float64(i),
			Float4:   4.0 + float64(i),
			Uint4:    uint32(i + 11),
			Bool5:    (i % 6) == 0,
			String16: "string16",
			String17: "string17",
			Int8:     int32(i + 12),
		}
	}

	outFile, err := os.Create("issue158.parquet")
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	writer := parquet.NewGenericWriter[Record](bufio.NewWriter(outFile))
	defer func() {
		if err := writer.Flush(); err != nil {
			log.Fatal("flushing:", err)
		}
		if err := writer.Close(); err != nil {
			log.Fatal("closing:", err)
		}
	}()

	for _, r := range input {
		if _, err := writer.Write([]Record{r}); err != nil {
			log.Fatal("writing:", err)
		}
	}
}

type Record struct {
	Uint1    uint32
	Uint2    uint32
	String1  string
	String2  string
	String3  string
	String4  string
	String5  string
	String6  string
	String7  string
	String8  string
	String9  string
	String10 string
	String11 string
	String12 string
	String13 string
	Bool1    bool
	Int1     int
	Int2     int
	Int3     int
	Int4     int
	Int5     int
	Int6     int
	Int7     int
	String14 string
	String15 string
	Bool2    bool
	Bool3    bool
	Bool4    bool
	Time1    time.Time
	Float1   float64
	Float2   float64
	Uint3    uint32
	Float3   float64
	Float4   float64
	Uint4    uint32
	Bool5    bool
	String16 string
	String17 string
	Int8     int32
}
