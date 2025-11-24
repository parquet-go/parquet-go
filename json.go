package parquet

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"

	"github.com/parquet-go/bitpack/unsafecast"
)

var (
	errEndOfObject           = errors.New("}")
	errEndOfArray            = errors.New("]")
	errUnexpectedEndOfObject = errors.New("unexpected end of object")
	errUnexpectedEndOfArray  = errors.New("unexpected end of array")
)

const (
	// UTF-16 surrogate pair boundaries (from Unicode standard)
	// Note: These are not valid Unicode scalar values, so we use hex literals
	surrogateMin    = 0xD800 // Start of high surrogate range
	lowSurrogateMin = 0xDC00 // Start of low surrogate range
	lowSurrogateMax = 0xDFFF // End of low surrogate range
)

const (
	// jsonKindShift is calculated based on pointer size to use the high bits
	// for the kind field. We have 7 jsonKind values (0-6), requiring 3 bits.
	// On 64-bit systems this is 61 (top 3 bits for kind, bottom 61 for length),
	// on 32-bit systems this is 29 (top 3 bits for kind, bottom 29 for length).
	jsonKindShift = (unsafe.Sizeof(uintptr(0))*8 - 3)
	jsonKindMask  = (1 << jsonKindShift) - 1
)

type jsonKind int

const (
	jsonNull jsonKind = iota
	jsonTrue
	jsonFalse
	jsonNumber
	jsonString
	jsonObject
	jsonArray
)

type jsonField struct {
	key string
	val jsonValue
}

type jsonValue struct {
	p unsafe.Pointer
	n uintptr
}

func (v *jsonValue) kind() jsonKind {
	return jsonKind(v.n >> jsonKindShift)
}

func (v *jsonValue) len() int {
	return int(v.n & jsonKindMask)
}

func (v *jsonValue) int() int64 {
	i, err := strconv.ParseInt(v.string(), 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}

func (v *jsonValue) uint() uint64 {
	u, err := strconv.ParseUint(v.string(), 10, 64)
	if err != nil {
		panic(err)
	}
	return u
}

func (v *jsonValue) float() float64 {
	f, err := strconv.ParseFloat(v.string(), 64)
	if err != nil {
		panic(err)
	}
	return f
}

func (v *jsonValue) string() string {
	return unsafe.String((*byte)(v.p), v.len())
}

func (v *jsonValue) bytes() []byte {
	return unsafe.Slice((*byte)(v.p), v.len())
}

func (v *jsonValue) array() []jsonValue {
	return unsafe.Slice((*jsonValue)(v.p), v.len())
}

func (v *jsonValue) object() []jsonField {
	return unsafe.Slice((*jsonField)(v.p), v.len())
}

func (v *jsonValue) lookup(k string) *jsonValue {
	fields := v.object()
	i, ok := slices.BinarySearchFunc(fields, k, func(a jsonField, b string) int {
		return strings.Compare(a.key, b)
	})
	if ok {
		return &fields[i].val
	}
	return nil
}

func jsonNullValue() jsonValue {
	return jsonValue{n: uintptr(jsonNull) << jsonKindShift}
}

func jsonTrueValue() jsonValue {
	return jsonValue{n: uintptr(jsonTrue)<<jsonKindShift | 1}
}

func jsonFalseValue() jsonValue {
	return jsonValue{n: uintptr(jsonFalse)<<jsonKindShift | 0}
}

func jsonNumberValue(s string) jsonValue {
	return jsonValue{
		p: unsafe.Pointer(unsafe.StringData(s)),
		n: (uintptr(jsonNumber) << jsonKindShift) | uintptr(len(s)),
	}
}

func jsonStringValue(s string) jsonValue {
	return jsonValue{
		p: unsafe.Pointer(unsafe.StringData(s)),
		n: (uintptr(jsonString) << jsonKindShift) | uintptr(len(s)),
	}
}

func jsonArrayValue(elements []jsonValue) jsonValue {
	return jsonValue{
		p: unsafe.Pointer(unsafe.SliceData(elements)),
		n: (uintptr(jsonArray) << jsonKindShift) | uintptr(len(elements)),
	}
}

func jsonObjectValue(fields []jsonField) jsonValue {
	return jsonValue{
		p: unsafe.Pointer(unsafe.SliceData(fields)),
		n: (uintptr(jsonObject) << jsonKindShift) | uintptr(len(fields)),
	}
}

type jsonTokenizer struct {
	json   string
	offset int
}

func (t *jsonTokenizer) next() (token string, ok bool) {
	for t.offset < len(t.json) {
		i := t.offset
		j := i + 1
		switch t.json[i] {
		case ' ', '\t', '\n', '\r':
			t.offset++
			continue
		case '[', ']', '{', '}', ':', ',':
			t.offset = j
			return t.json[i:j], true
		case '"':
			for {
				k := strings.IndexByte(t.json[j:], '"')
				if k < 0 {
					j = len(t.json)
					break
				}
				j += k + 1
				for k = j - 2; k > i && t.json[k] == '\\'; k-- {
				}
				if (j-k)%2 == 0 {
					break
				}
			}
			t.offset = j
			return t.json[i:j], true
		default:
			for j < len(t.json) {
				switch t.json[j] {
				case ' ', '\t', '\n', '\r', '[', ']', '{', '}', ':', ',', '"':
					t.offset = j
					return t.json[i:j], true
				}
				j++
			}
			t.offset = j
			return t.json[i:j], true
		}
	}
	return "", false
}

func jsonParse(data []byte) (*jsonValue, error) {
	if len(data) == 0 {
		v := jsonNullValue()
		return &v, nil
	}
	tokenizer := &jsonTokenizer{json: unsafecast.String(data)}
	v, err := jsonParseTokens(tokenizer)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func jsonParseTokens(tokens *jsonTokenizer) (jsonValue, error) {
	token, ok := tokens.next()
	if !ok {
		return jsonValue{}, errUnexpectedEndOfObject
	}
	switch token[0] {
	case 'n':
		if token != "null" {
			return jsonValue{}, fmt.Errorf("invalid token: %q", token)
		}
		return jsonNullValue(), nil
	case 't':
		if token != "true" {
			return jsonValue{}, fmt.Errorf("invalid token: %q", token)
		}
		return jsonTrueValue(), nil
	case 'f':
		if token != "false" {
			return jsonValue{}, fmt.Errorf("invalid token: %q", token)
		}
		return jsonFalseValue(), nil
	case '"':
		s, err := unquote(token)
		if err != nil {
			return jsonValue{}, fmt.Errorf("invalid token: %q", token)
		}
		return jsonStringValue(s), nil
	case '[':
		return jsonParseArray(tokens)
	case '{':
		return jsonParseObject(tokens)
	case ']':
		return jsonValue{}, errEndOfArray
	case '}':
		return jsonValue{}, errEndOfObject
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return jsonNumberValue(token), nil
	default:
		return jsonValue{}, fmt.Errorf("invalid token: %q", token)
	}
}

func jsonParseArray(tokens *jsonTokenizer) (jsonValue, error) {
	elements := make([]jsonValue, 0, 8)

	for i := 0; ; i++ {
		if i != 0 {
			token, ok := tokens.next()
			if !ok {
				return jsonValue{}, errUnexpectedEndOfArray
			}
			if token == "]" {
				break
			}
			if token != "," {
				return jsonValue{}, fmt.Errorf("expected ',' or ']', got %q", token)
			}
		}

		v, err := jsonParseTokens(tokens)
		if err != nil {
			if i == 0 && errors.Is(err, errEndOfArray) {
				return jsonArrayValue(elements), nil
			}
			return jsonValue{}, err
		}
		elements = append(elements, v)
	}

	return jsonArrayValue(elements), nil
}

func jsonParseObject(tokens *jsonTokenizer) (jsonValue, error) {
	fields := make([]jsonField, 0, 8)

	for i := 0; ; i++ {
		if i != 0 {
			token, ok := tokens.next()
			if !ok {
				return jsonValue{}, errUnexpectedEndOfObject
			}
			if token == "}" {
				break
			}
			if token != "," {
				return jsonValue{}, fmt.Errorf("expected ',' or '}', got %q", token)
			}
		}

		token, ok := tokens.next()
		if !ok {
			return jsonValue{}, errUnexpectedEndOfObject
		}
		if i == 0 && token == "}" {
			return jsonObjectValue(fields), nil
		}
		if token[0] != '"' {
			return jsonValue{}, fmt.Errorf("expected string key, got %q", token)
		}
		key, err := unquote(token)
		if err != nil {
			return jsonValue{}, fmt.Errorf("invalid key: %q: %w", token, err)
		}

		token, ok = tokens.next()
		if !ok {
			return jsonValue{}, errUnexpectedEndOfObject
		}
		if token != ":" {
			return jsonValue{}, fmt.Errorf("%q → expected ':', got %q", key, token)
		}

		val, err := jsonParseTokens(tokens)
		if err != nil {
			return jsonValue{}, fmt.Errorf("%q → %w", key, err)
		}
		fields = append(fields, jsonField{key: key, val: val})
	}

	slices.SortFunc(fields, func(a, b jsonField) int {
		return strings.Compare(a.key, b.key)
	})

	return jsonObjectValue(fields), nil
}

func jsonFormat(buf []byte, val *jsonValue) []byte {
	switch val.kind() {
	case jsonNull:
		return append(buf, "null"...)

	case jsonTrue:
		return append(buf, "true"...)

	case jsonFalse:
		return append(buf, "false"...)

	case jsonNumber:
		return strconv.AppendFloat(buf, val.float(), 'g', -1, 64)

	case jsonString:
		return strconv.AppendQuote(buf, val.string())

	case jsonArray:
		buf = append(buf, '[')
		array := val.array()
		for i := range array {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = jsonFormat(buf, &array[i])
		}
		return append(buf, ']')

	case jsonObject:
		buf = append(buf, '{')
		fields := val.object()
		for i := range fields {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendQuote(buf, fields[i].key)
			buf = append(buf, ':')
			buf = jsonFormat(buf, &fields[i].val)
		}
		return append(buf, '}')

	default:
		return buf
	}
}

func unquote(s string) (string, error) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", fmt.Errorf("invalid quoted string: %s", s)
	}
	s = s[1 : len(s)-1]

	if strings.IndexByte(s, '\\') < 0 {
		return s, nil
	}

	b := make([]byte, 0, len(s))
	for {
		i := strings.IndexByte(s, '\\')
		if i < 0 {
			b = append(b, s...)
			break
		}
		b = append(b, s[:i]...)
		if i+1 >= len(s) {
			return "", fmt.Errorf("invalid escape sequence at end of string")
		}
		switch c := s[i+1]; c {
		case '"', '\\', '/':
			b = append(b, c)
			s = s[i+2:]
		case 'b':
			b = append(b, '\b')
			s = s[i+2:]
		case 'f':
			b = append(b, '\f')
			s = s[i+2:]
		case 'n':
			b = append(b, '\n')
			s = s[i+2:]
		case 'r':
			b = append(b, '\r')
			s = s[i+2:]
		case 't':
			b = append(b, '\t')
			s = s[i+2:]
		case 'u':
			if i+6 > len(s) {
				return "", fmt.Errorf("invalid unicode escape sequence")
			}
			r, err := strconv.ParseUint(s[i+2:i+6], 16, 16)
			if err != nil {
				return "", fmt.Errorf("invalid unicode escape sequence: %w", err)
			}

			r1 := rune(r)
			// Check for UTF-16 surrogate pair using utf16 package
			if utf16.IsSurrogate(r1) {
				// Low surrogate without high surrogate is an error
				if r1 >= lowSurrogateMin {
					return "", fmt.Errorf("invalid surrogate pair: unexpected low surrogate")
				}
				// High surrogate, look for low surrogate
				if i+12 > len(s) || s[i+6] != '\\' || s[i+7] != 'u' {
					return "", fmt.Errorf("invalid surrogate pair: missing low surrogate")
				}
				low, err := strconv.ParseUint(s[i+8:i+12], 16, 16)
				if err != nil {
					return "", fmt.Errorf("invalid unicode escape sequence in surrogate pair: %w", err)
				}
				r2 := rune(low)
				if r2 < lowSurrogateMin || r2 > lowSurrogateMax {
					return "", fmt.Errorf("invalid surrogate pair: low surrogate out of range")
				}
				// Decode the surrogate pair using utf16 package
				decoded := utf16.DecodeRune(r1, r2)
				b = utf8.AppendRune(b, decoded)
				s = s[i+12:]
			} else {
				b = utf8.AppendRune(b, r1)
				s = s[i+6:]
			}
		default:
			return "", fmt.Errorf("invalid escape character: %q", c)
		}
	}
	return string(b), nil
}
