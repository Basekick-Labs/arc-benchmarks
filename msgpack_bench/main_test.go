package msgpack_bench

import (
	"math/rand"
	"testing"
	"time"

	vmsgpack "github.com/Basekick-Labs/msgpack/v6"
	ugorji "github.com/ugorji/go/codec"
)

// ---------------------------------------------------------------------------
// Data fixtures matching Arc's real data shapes
// ---------------------------------------------------------------------------

// smallRecord: minimal ingest payload (1 record, 3 fields)
func smallRecord() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"m": "cpu",
			"t": time.Now().UnixMilli(),
			"fields": map[string]interface{}{
				"usage_idle":   95.2,
				"usage_system": 2.1,
				"usage_user":   2.7,
			},
			"tags": map[string]string{
				"host":   "server01",
				"region": "us-east-1",
			},
		},
	}
}

// rowBatch: 100 records typical WAL batch
func rowBatch() []map[string]interface{} {
	records := make([]map[string]interface{}, 100)
	ts := time.Now().UnixMilli()
	for i := range records {
		records[i] = map[string]interface{}{
			"m": "cpu",
			"t": ts + int64(i),
			"h": "server01",
			"fields": map[string]interface{}{
				"usage_idle":   rand.Float64() * 100,
				"usage_system": rand.Float64() * 20,
				"usage_user":   rand.Float64() * 80,
				"usage_iowait": rand.Float64() * 10,
				"load1":        rand.Float64() * 4,
				"load5":        rand.Float64() * 4,
				"load15":       rand.Float64() * 4,
				"n_cpus":       int64(16),
			},
			"tags": map[string]string{
				"host":   "server01",
				"region": "us-east-1",
				"dc":     "dc1",
				"rack":   "rack42",
			},
		}
	}
	return records
}

// columnarRecord: columnar ingest format with 5 columns, 1000 values each
func columnarRecord() map[string]interface{} {
	n := 1000
	times := make([]interface{}, n)
	vals := make([]interface{}, n)
	regions := make([]interface{}, n)
	hosts := make([]interface{}, n)
	loads := make([]interface{}, n)
	ts := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		times[i] = ts + int64(i)
		vals[i] = rand.Float64() * 100
		regions[i] = "us-east-1"
		hosts[i] = "server01"
		loads[i] = rand.Float64() * 4
	}
	return map[string]interface{}{
		"m": "cpu",
		"columns": map[string][]interface{}{
			"time":   times,
			"value":  vals,
			"region": regions,
			"host":   hosts,
			"load":   loads,
		},
	}
}

// ---------------------------------------------------------------------------
// Helpers for ugorji codec
// ---------------------------------------------------------------------------

var msgpackHandle = &ugorji.MsgpackHandle{}

func ugorjiMarshal(v interface{}) ([]byte, error) {
	var out []byte
	enc := ugorji.NewEncoderBytes(&out, msgpackHandle)
	err := enc.Encode(v)
	return out, err
}

func ugorjiUnmarshal(data []byte, v interface{}) error {
	dec := ugorji.NewDecoderBytes(data, msgpackHandle)
	return dec.Decode(v)
}

// ---------------------------------------------------------------------------
// Benchmarks: Small Record
// ---------------------------------------------------------------------------

func BenchmarkVmihailenco_Marshal_Small(b *testing.B) {
	data := smallRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vmsgpack.Marshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUgorji_Marshal_Small(b *testing.B) {
	data := smallRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ugorjiMarshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVmihailenco_Unmarshal_Small(b *testing.B) {
	data := smallRecord()
	encoded, _ := vmsgpack.Marshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []map[string]interface{}
		if err := vmsgpack.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUgorji_Unmarshal_Small(b *testing.B) {
	data := smallRecord()
	encoded, _ := ugorjiMarshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []map[string]interface{}
		if err := ugorjiUnmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: Row Batch (100 records — WAL write path)
// ---------------------------------------------------------------------------

func BenchmarkVmihailenco_Marshal_RowBatch(b *testing.B) {
	data := rowBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vmsgpack.Marshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUgorji_Marshal_RowBatch(b *testing.B) {
	data := rowBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ugorjiMarshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVmihailenco_Unmarshal_RowBatch(b *testing.B) {
	data := rowBatch()
	encoded, _ := vmsgpack.Marshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []map[string]interface{}
		if err := vmsgpack.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUgorji_Unmarshal_RowBatch(b *testing.B) {
	data := rowBatch()
	encoded, _ := ugorjiMarshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []map[string]interface{}
		if err := ugorjiUnmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: Columnar Record (ingest fast-path)
// ---------------------------------------------------------------------------

func BenchmarkVmihailenco_Marshal_Columnar(b *testing.B) {
	data := columnarRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vmsgpack.Marshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUgorji_Marshal_Columnar(b *testing.B) {
	data := columnarRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ugorjiMarshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVmihailenco_Unmarshal_Columnar(b *testing.B) {
	data := columnarRecord()
	encoded, _ := vmsgpack.Marshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out map[string]interface{}
		if err := vmsgpack.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUgorji_Unmarshal_Columnar(b *testing.B) {
	data := columnarRecord()
	encoded, _ := ugorjiMarshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out map[string]interface{}
		if err := ugorjiUnmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Cross-compatibility: unmarshal data encoded by the other library
// ---------------------------------------------------------------------------

func BenchmarkCross_VmihailencoEncode_UgorjiDecode_RowBatch(b *testing.B) {
	data := rowBatch()
	encoded, _ := vmsgpack.Marshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []map[string]interface{}
		if err := ugorjiUnmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCross_UgorjiEncode_VmihailencoDecode_RowBatch(b *testing.B) {
	data := rowBatch()
	encoded, _ := ugorjiMarshal(data)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []map[string]interface{}
		if err := vmsgpack.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}
