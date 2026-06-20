package msgpack_bench

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
)

// ---------------------------------------------------------------------------
// Benchmarks: MessagePack encoding of the same QueryResponse fixtures used
// by json_test.go. The production endpoint at internal/api/query_msgpack.go
// uses streaming encode (EncodeMapLen + per-row loop) rather than the
// full-document Encode(struct), so we benchmark both paths to map the
// experiment's headline result to the production path.
// ---------------------------------------------------------------------------

// BenchmarkMsgpack_Marshal_SmallQuery encodes the entire struct in one
// shot via the reflect-based Encode path (the simplest possible client-
// side API). This is what a user calling `msgpack.Marshal(resp)` pays.
func BenchmarkMsgpack_Marshal_SmallQuery(b *testing.B) {
	resp := smallQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msgpack.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMsgpack_Marshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msgpack.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMsgpack_Stream_SmallQuery mirrors the production handler shape:
// allocate a pooled encoder, reset it on a writer, write a map of fixed
// length, and stream each row as a typed array.
//
// Caveat: this bench iterates rows-as-[]interface{} and dispatches via a
// type-switch on each cell, because the bench module deliberately avoids
// the Arrow dep. The production handler at internal/api/query_msgpack.go
// iterates **typed Arrow column arrays directly** (e.g. *array.Int64) —
// skipping the type assertion entirely. So the numbers below are a
// PESSIMISTIC lower bound on the production win; the real handler is
// slightly faster than what this bench reports. The gap between the
// JSON and msgpack columns is the honest result.
func BenchmarkMsgpack_Stream_SmallQuery(b *testing.B) {
	resp := smallQueryResponse()
	// Measure one encode to learn the byte size; SetBytes converts the
	// ns/op number to MB/s in -bench output. The cost of one extra
	// encode is amortised across b.N.
	var sizeBuf bytes.Buffer
	encodeStreaming(&sizeBuf, &resp)
	b.SetBytes(int64(sizeBuf.Len()))
	b.ReportAllocs()
	b.ResetTimer()

	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		buf.Reset()
		encodeStreaming(&buf, &resp)
	}
}

func BenchmarkMsgpack_Stream_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	var sizeBuf bytes.Buffer
	encodeStreaming(&sizeBuf, &resp)
	b.SetBytes(int64(sizeBuf.Len()))
	b.ReportAllocs()
	b.ResetTimer()

	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		buf.Reset()
		encodeStreaming(&buf, &resp)
	}
}

// BenchmarkMsgpack_Unmarshal_LargeQuery is the round-trip cost a client
// pays after the server-side encode. Useful for the "total wall-clock
// JSON-vs-msgpack" comparison.
func BenchmarkMsgpack_Unmarshal_LargeQuery(b *testing.B) {
	resp := largeQueryResponse()
	data, err := msgpack.Marshal(resp)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out QueryResponse
		if err := msgpack.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Wire-size checks. Not Go benchmarks — printed via t.Log so the numbers
// land in the `go test -v` output alongside the bench results.
// ---------------------------------------------------------------------------

func TestWireSize_SmallQuery(t *testing.T) {
	resp := smallQueryResponse()
	jb, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	mpb, err := msgpack.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var mpStream bytes.Buffer
	encodeStreaming(&mpStream, &resp)

	t.Logf("Small query wire sizes — json=%d bytes  msgpack(struct)=%d bytes  msgpack(stream)=%d bytes  json/msgpack-stream=%.2fx",
		len(jb), len(mpb), mpStream.Len(),
		float64(len(jb))/float64(mpStream.Len()))
}

func TestWireSize_LargeQuery(t *testing.T) {
	resp := largeQueryResponse()
	jb, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	mpb, err := msgpack.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var mpStream bytes.Buffer
	encodeStreaming(&mpStream, &resp)

	t.Logf("Large query wire sizes — json=%d bytes  msgpack(struct)=%d bytes  msgpack(stream)=%d bytes  json/msgpack-stream=%.2fx",
		len(jb), len(mpb), mpStream.Len(),
		float64(len(jb))/float64(mpStream.Len()))
}

// ---------------------------------------------------------------------------
// Internal: encodeStreaming mirrors the production streamArrowMsgPackColumnar
// structure — `data` is an array of per-column arrays. One type-switch per
// column drives a tight typed loop over its N values; no per-cell type
// assertion. The wire format matches what the production endpoint emits.
// ---------------------------------------------------------------------------

func encodeStreaming(buf *bytes.Buffer, resp *QueryResponse) {
	enc := msgpack.GetEncoder()
	enc.Reset(buf)
	defer msgpack.PutEncoder(enc)

	numCols := len(resp.Columns)
	numRows := len(resp.Data)

	_ = enc.EncodeMapLen(7)

	_ = enc.EncodeString("success")
	_ = enc.EncodeBool(resp.Success)

	_ = enc.EncodeString("columns")
	_ = enc.EncodeArrayLen(numCols)
	for _, c := range resp.Columns {
		_ = enc.EncodeString(c)
	}

	// "types": infer per-column from the first non-nil cell. Production
	// uses arrow.DataType.String(); the microbench fixtures use Go
	// primitives, so we pick a stable canonical name per Go type.
	_ = enc.EncodeString("types")
	_ = enc.EncodeArrayLen(numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		typeName := "null"
		for _, row := range resp.Data {
			if colIdx >= len(row) || row[colIdx] == nil {
				continue
			}
			switch row[colIdx].(type) {
			case bool:
				typeName = "bool"
			case int, int32, int64:
				typeName = "int64"
			case uint, uint32, uint64:
				typeName = "uint64"
			case float32, float64:
				typeName = "float64"
			case string:
				typeName = "string"
			case []byte:
				typeName = "binary"
			default:
				typeName = "string"
			}
			break
		}
		_ = enc.EncodeString(typeName)
	}

	// "data": columnar. Per column, type-switch once and run a tight
	// typed loop over N values. Allocates a numRows-long scratch slice
	// per column (negligible vs the per-cell type assertion savings).
	_ = enc.EncodeString("data")
	_ = enc.EncodeArrayLen(numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		_ = enc.EncodeArrayLen(numRows)
		// Type-switch on the first non-nil cell so subsequent values
		// flow through the typed path with no interface dispatch.
		var probe interface{}
		for _, row := range resp.Data {
			if colIdx < len(row) && row[colIdx] != nil {
				probe = row[colIdx]
				break
			}
		}
		switch probe.(type) {
		case bool:
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.EncodeBool(v.(bool))
				}
			}
		case int:
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.EncodeInt(int64(v.(int)))
				}
			}
		case int64:
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.EncodeInt(v.(int64))
				}
			}
		case uint64:
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.EncodeUint(v.(uint64))
				}
			}
		case float64:
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.EncodeFloat64(v.(float64))
				}
			}
		case string:
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.EncodeString(v.(string))
				}
			}
		case []byte:
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.EncodeBytes(v.([]byte))
				}
			}
		default:
			// Fallback for column whose first non-nil value is some
			// unhandled type — emit each cell reflect-encoded.
			for _, row := range resp.Data {
				v := row[colIdx]
				if v == nil {
					_ = enc.EncodeNil()
				} else {
					_ = enc.Encode(v)
				}
			}
		}
	}

	_ = enc.EncodeString("row_count")
	_ = enc.EncodeUint(uint64(resp.RowCount))

	_ = enc.EncodeString("execution_time_ms")
	_ = enc.EncodeUint(uint64(resp.ExecutionTimeMs))

	_ = enc.EncodeString("timestamp")
	_ = enc.EncodeString(resp.Timestamp)
}
