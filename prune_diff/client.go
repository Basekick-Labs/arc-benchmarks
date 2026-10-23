package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// arcClient talks to one Arc node.
type arcClient struct {
	base  string
	token string
	http  *http.Client
}

func newArcClient(base, token string) *arcClient {
	return &arcClient{base: strings.TrimRight(base, "/"), token: token, http: &http.Client{Timeout: 60 * time.Second}}
}

func (a *arcClient) do(ctx context.Context, method, path string, body []byte, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	return a.http.Do(req)
}

// writeLP posts line protocol at microsecond precision.
func (a *arcClient) writeLP(ctx context.Context, db string, body []byte) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	resp, err := a.do(ctx, "POST", "/api/v1/write/line-protocol?precision=us", body, map[string]string{"x-arc-database": db, "Content-Type": "text/plain"})
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	return resp.StatusCode, drain(resp.Body), nil
}

// flush asks Arc to flush its line-protocol buffers.
func (a *arcClient) flush(ctx context.Context) error {
	resp, err := a.do(ctx, "POST", "/api/v1/write/line-protocol/flush", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("flush: %d %s", resp.StatusCode, drain(resp.Body))
	}
	return nil
}

// wire is a query transport.
type wire string

const (
	wireJSON    wire = "json"
	wireArrow   wire = "arrow"
	wireMsgPack wire = "msgpack"
)

// result is a query answer normalised to strings: every value the corpus
// selects is an integer (counts, sums, seq, epoch_us buckets).
type result struct {
	rows [][]string
	// status is the HTTP status; retryAfter is set on a 503 that carried it.
	status     int
	retryAfter bool
	// marker is a completeness marker the answer carried (truncated,
	// rows_capped): a harness error, not a wrong count.
	marker string
	err    error
}

// query runs sql over w. db is sent as x-arc-database when header is true.
func (a *arcClient) query(ctx context.Context, w wire, sql, db string, header bool) result {
	path := "/api/v1/query"
	if w != wireJSON {
		path += "/" + string(w)
	}
	body, _ := json.Marshal(map[string]string{"sql": sql})
	hdr := map[string]string{"Content-Type": "application/json"}
	if header {
		hdr["x-arc-database"] = db
	}
	resp, err := a.do(ctx, "POST", path, body, hdr)
	if err != nil {
		return result{err: err}
	}
	defer resp.Body.Close()
	res := result{status: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After") != ""}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		res.err = err
		return res
	}
	if resp.StatusCode != http.StatusOK {
		res.err = fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(raw), 300))
		return res
	}
	switch w {
	case wireJSON:
		res.rows, res.marker, res.err = decodeJSON(raw)
	case wireArrow:
		res.rows, res.err = decodeArrow(raw)
	case wireMsgPack:
		res.rows, res.err = decodeMsgPack(raw)
	}
	return res
}

func decodeJSON(raw []byte) ([][]string, string, error) {
	var env struct {
		Success   bool            `json:"success"`
		Error     string          `json:"error"`
		Data      [][]json.Number `json:"data"`
		Truncated *bool           `json:"truncated"`
		Capped    *bool           `json:"rows_capped"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		return nil, "", fmt.Errorf("decode json: %w (%s)", err, truncate(string(raw), 200))
	}
	if !env.Success {
		return nil, "", fmt.Errorf("query failed: %s", env.Error)
	}
	marker := ""
	if env.Truncated != nil && *env.Truncated {
		marker = "truncated"
	}
	if env.Capped != nil && *env.Capped {
		marker = "rows_capped"
	}
	out := make([][]string, len(env.Data))
	for i, r := range env.Data {
		out[i] = make([]string, len(r))
		for j, v := range r {
			out[i][j] = v.String()
		}
	}
	return out, marker, nil
}

func decodeArrow(raw []byte) ([][]string, error) {
	rd, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("arrow reader: %w", err)
	}
	defer rd.Release()
	var out [][]string
	for rd.Next() {
		rec := rd.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			r := make([]string, rec.NumCols())
			for c := 0; c < int(rec.NumCols()); c++ {
				r[c] = arrowValue(rec.Column(c), i)
			}
			out = append(out, r)
		}
	}
	return out, rd.Err()
}

func arrowValue(col arrow.Array, i int) string {
	if col.IsNull(i) {
		return "null"
	}
	switch c := col.(type) {
	case *array.Int64:
		return strconv.FormatInt(c.Value(i), 10)
	case *array.Int32:
		return strconv.FormatInt(int64(c.Value(i)), 10)
	case *array.Uint64:
		return strconv.FormatUint(c.Value(i), 10)
	case *array.Float64:
		return strconv.FormatFloat(c.Value(i), 'f', -1, 64)
	case *array.Decimal128:
		return c.Value(i).BigInt().String()
	default:
		return col.ValueStr(i)
	}
}

func decodeMsgPack(raw []byte) ([][]string, error) {
	var env map[string]any
	if err := msgpack.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode msgpack: %w", err)
	}
	if ok, _ := env["success"].(bool); !ok {
		return nil, fmt.Errorf("query failed: %v", env["error"])
	}
	cols, _ := env["data"].([]any)
	if len(cols) == 0 {
		return nil, nil
	}
	n := len(cols[0].([]any))
	out := make([][]string, n)
	for i := range out {
		out[i] = make([]string, len(cols))
		for c := range cols {
			out[i][c] = fmt.Sprint(cols[c].([]any)[i])
		}
	}
	return out, nil
}

// metrics scrapes /metrics into name -> value (the families the report
// tracks; labels are summed).
func (a *arcClient) metrics(ctx context.Context, prefixes ...string) (map[string]float64, error) {
	resp, err := a.do(ctx, "GET", "/metrics", nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		keep := false
		for _, p := range prefixes {
			if strings.HasPrefix(line, p) {
				keep = true
				break
			}
		}
		if !keep {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		f := strings.Fields(line)
		v, err := strconv.ParseFloat(f[len(f)-1], 64)
		if err != nil {
			continue
		}
		out[name] += v
	}
	return out, sc.Err()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
