package dalgo2cosmosdb

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"unicode/utf8"
)

// jsonAdmission counts the actual encoding/json wire representation while
// copying only closed native values. Repeated shared strings are charged once
// per wire occurrence; neither a huge marshal buffer nor a graph-dedup shortcut
// is allowed before aggregate admission. No custom Marshaler is invoked.
type jsonAdmission struct {
	op                     *operation
	remaining, used, nodes int
}

func (a *jsonAdmission) charge(n int) error {
	if e := a.op.check(); e != nil {
		return e
	}
	if n < 0 || n > a.remaining {
		return ErrLimit
	}
	a.remaining -= n
	a.used += n
	return nil
}
func (a *jsonAdmission) minimum(n int) error {
	if e := a.op.check(); e != nil {
		return e
	}
	if n < 0 || n > a.remaining {
		return ErrLimit
	}
	return nil
}
func (a *jsonAdmission) string(value string) error {
	if !utf8.ValidString(value) || len(value) > 1024*1024 {
		return ErrRequest
	}
	if e := a.charge(2); e != nil {
		return e
	}
	for _, r := range value {
		n := utf8.RuneLen(r)
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			n = 2
		case '<', '>', '&', '\u2028', '\u2029':
			n = 6
		default:
			if r < 32 {
				n = 6
			}
		}
		if e := a.charge(n); e != nil {
			return e
		}
	}
	return nil
}
func (a *jsonAdmission) number(x json.Number) (any, error) {
	if len(x) == 0 || len(x) > 128 || !json.Valid([]byte(x)) || (x[0] != '-' && (x[0] < '0' || x[0] > '9')) {
		return nil, ErrRequest
	}
	if e := a.charge(len(x)); e != nil {
		return nil, e
	}
	return x, nil
}
func (a *jsonAdmission) value(v any, depth int) (any, error) {
	a.nodes++
	if depth > 32 || a.nodes > 65536 {
		return nil, ErrLimit
	}
	if e := a.op.check(); e != nil {
		return nil, e
	}
	switch x := v.(type) {
	case nil:
		if e := a.charge(4); e != nil {
			return nil, e
		}
		return nil, nil
	case bool:
		n := 5
		if x {
			n = 4
		}
		if e := a.charge(n); e != nil {
			return nil, e
		}
		return x, nil
	case string:
		if e := a.string(x); e != nil {
			return nil, e
		}
		return x, nil
	case json.Number:
		return a.number(x)
	case int:
		return a.number(json.Number(strconv.Itoa(x)))
	case int64:
		return a.number(json.Number(strconv.FormatInt(x, 10)))
	case uint64:
		return a.number(json.Number(strconv.FormatUint(x, 10)))
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, ErrRequest
		}
		return a.number(json.Number(strconv.FormatFloat(x, 'g', -1, 64)))
	case []any:
		if len(x) > 65536 {
			return nil, ErrLimit
		}
		punctuation := 2
		if len(x) > 0 {
			punctuation += len(x) - 1
		}
		if e := a.minimum(punctuation + len(x)); e != nil {
			return nil, e
		}
		if e := a.charge(punctuation); e != nil {
			return nil, e
		}
		out := make([]any, len(x))
		for i, v := range x {
			copy, e := a.value(v, depth+1)
			if e != nil {
				return nil, e
			}
			out[i] = copy
		}
		return out, nil
	case map[string]any:
		if len(x) > 65536 {
			return nil, ErrLimit
		}
		punctuation := 2
		if len(x) > 0 {
			punctuation += 2*len(x) - 1
		}
		if e := a.minimum(punctuation + 3*len(x)); e != nil {
			return nil, e
		}
		if e := a.charge(punctuation); e != nil {
			return nil, e
		}
		out := make(map[string]any, len(x))
		for k, v := range x {
			a.nodes++
			if a.nodes > 65536 {
				return nil, ErrLimit
			}
			if e := a.string(k); e != nil {
				return nil, e
			}
			copy, e := a.value(v, depth+1)
			if e != nil {
				return nil, e
			}
			out[k] = copy
		}
		return out, nil
	default:
		return nil, ErrRequest
	}
}

// JSON's escaped surrogate pairs must be well-formed. encoding/json otherwise
// silently replaces lone surrogates; native documents must not be corrupted.
func validSurrogates(raw []byte) bool {
	in := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			in = !in
			continue
		}
		if !in || raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, e := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if n >= 0xDC00 && n <= 0xDFFF {
			return false
		}
		if n >= 0xD800 && n <= 0xDBFF {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, e := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if e != nil || low < 0xDC00 || low > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}
func decodeJSON(raw []byte) (any, error) {
	if !utf8.Valid(raw) || !validSurrogates(raw) {
		return nil, ErrResponse
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	nodes := 0
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		nodes++
		if depth > 32 || nodes > 65536 {
			return nil, ErrLimit
		}
		token, e := d.Token()
		if e != nil {
			return nil, ErrResponse
		}
		switch t := token.(type) {
		case json.Delim:
			switch t {
			case '{':
				out := make(map[string]any)
				for d.More() {
					name, e := d.Token()
					k, ok := name.(string)
					if e != nil || !ok || len(k) > 1024*1024 {
						return nil, ErrResponse
					}
					nodes++
					if _, exists := out[k]; exists {
						return nil, ErrResponse
					}
					v, e := read(depth + 1)
					if e != nil {
						return nil, e
					}
					out[k] = v
				}
				end, e := d.Token()
				if e != nil || end != json.Delim('}') {
					return nil, ErrResponse
				}
				return out, nil
			case '[':
				out := make([]any, 0)
				for d.More() {
					v, e := read(depth + 1)
					if e != nil {
						return nil, e
					}
					out = append(out, v)
				}
				end, e := d.Token()
				if e != nil || end != json.Delim(']') {
					return nil, ErrResponse
				}
				return out, nil
			default:
				return nil, ErrResponse
			}
		case string:
			if len(t) > 1024*1024 {
				return nil, ErrLimit
			}
			return t, nil
		case json.Number:
			if len(t) > 128 {
				return nil, ErrLimit
			}
			return t, nil
		case nil, bool:
			return t, nil
		default:
			return nil, ErrResponse
		}
	}
	value, e := read(0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, ErrResponse
	}
	return value, nil
}
