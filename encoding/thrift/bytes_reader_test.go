package thrift_test

import (
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

type bytesReaderStruct struct {
	Name string `thrift:"1,required"`
	Data []byte `thrift:"2,required"`
}

func TestBytesReaderResetBytes(t *testing.T) {
	protocols := []struct {
		scenario string
		protocol thrift.Protocol
	}{
		{"compact", new(thrift.CompactProtocol)},
		{"binary", new(thrift.BinaryProtocol)},
	}

	for _, p := range protocols {
		t.Run(p.scenario, func(t *testing.T) {
			inputs := []bytesReaderStruct{
				{Name: "first", Data: []byte{1, 2, 3}},
				{Name: "second", Data: []byte{4}},
				{Name: "third", Data: []byte{5, 6}},
			}

			encoded := make([][]byte, len(inputs))
			for i, in := range inputs {
				b, err := thrift.Marshal(p.protocol, in)
				if err != nil {
					t.Fatal(err)
				}
				encoded[i] = b
			}

			// One reader and one decoder, reused across every input.
			reader := p.protocol.NewReaderFromBytes(nil).(thrift.BytesReader)
			decoder := thrift.NewDecoder(reader)

			for i, b := range encoded {
				var out bytesReaderStruct
				reader.ResetBytes(b)
				if err := decoder.Decode(&out); err != nil {
					t.Fatalf("input %d: %v", i, err)
				}
				if n := len(b) - reader.BytesRead(); n != 0 {
					t.Errorf("input %d: %d trailing bytes", i, n)
				}
				if out.Name != inputs[i].Name || string(out.Data) != string(inputs[i].Data) {
					t.Errorf("input %d: decoded %+v, want %+v", i, out, inputs[i])
				}
			}
		})
	}
}

// TestBytesReaderDecodedValuesAliasInput documents the contract that makes
// reusing a reader worthwhile, and the reason Unmarshal clones its input.
func TestBytesReaderDecodedValuesAliasInput(t *testing.T) {
	in := bytesReaderStruct{Name: "x", Data: []byte{1, 2, 3}}
	b, err := thrift.Marshal(new(thrift.CompactProtocol), in)
	if err != nil {
		t.Fatal(err)
	}

	reader := new(thrift.CompactProtocol).NewReaderFromBytes(nil).(thrift.BytesReader)
	var out bytesReaderStruct
	reader.ResetBytes(b)
	if err := thrift.NewDecoder(reader).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != "\x01\x02\x03" {
		t.Fatalf("decoded %v", out.Data)
	}

	// Mutating the input is visible through the decoded value: it aliases b.
	for i := range b {
		b[i] = 0xff
	}
	if string(out.Data) == "\x01\x02\x03" {
		t.Error("decoded bytes did not alias the input; the reader copied, and Unmarshal need not clone")
	}

	// Unmarshal, by contrast, clones and so is immune.
	b2, _ := thrift.Marshal(new(thrift.CompactProtocol), in)
	var safe bytesReaderStruct
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), b2, &safe); err != nil {
		t.Fatal(err)
	}
	for i := range b2 {
		b2[i] = 0xff
	}
	if string(safe.Data) != "\x01\x02\x03" {
		t.Errorf("Unmarshal did not isolate its caller from the input: %v", safe.Data)
	}
}
