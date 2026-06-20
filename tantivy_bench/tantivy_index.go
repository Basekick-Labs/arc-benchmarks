package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tantivy "github.com/anyproto/tantivy-go"
)

// TantivyIndex wraps a Tantivy index for benchmarking.
type TantivyIndex struct {
	ctx     *tantivy.TantivyContext
	schema  *tantivy.Schema
	path    string
	isNgram bool
}

// InitTantivy initializes the Tantivy library. Call once at startup.
func InitTantivy() {
	tantivy.LibInit(true, false, "warn")
}

// NewTantivyIndex creates a Tantivy index with the given tokenizer configuration.
func NewTantivyIndex(indexPath string, useNgram bool) (*TantivyIndex, error) {
	builder, err := tantivy.NewSchemaBuilder()
	if err != nil {
		return nil, fmt.Errorf("schema builder: %w", err)
	}

	// doc_id: stored + fast, raw tokenizer (exact match retrieval)
	if err := builder.AddTextField("doc_id", true, false, true, tantivy.IndexRecordOptionBasic, tantivy.TokenizerRaw); err != nil {
		return nil, fmt.Errorf("add doc_id: %w", err)
	}

	// message: the primary search target — indexed with chosen tokenizer
	tokenizerName := tantivy.TokenizerSimple
	if useNgram {
		tokenizerName = tantivy.TokenizerNgram
	}
	if err := builder.AddTextField("message", true, true, false, tantivy.IndexRecordOptionWithFreqsAndPositions, tokenizerName); err != nil {
		return nil, fmt.Errorf("add message: %w", err)
	}

	// Tag fields: stored + fast, raw tokenizer (exact match for tag filtering)
	for _, field := range []string{"level", "service", "host"} {
		if err := builder.AddTextField(field, true, false, true, tantivy.IndexRecordOptionBasic, tantivy.TokenizerRaw); err != nil {
			return nil, fmt.Errorf("add %s: %w", field, err)
		}
	}

	schema, err := builder.BuildSchema()
	if err != nil {
		return nil, fmt.Errorf("build schema: %w", err)
	}

	tc, err := tantivy.NewTantivyContextWithSchema(indexPath, schema)
	if err != nil {
		return nil, fmt.Errorf("create context: %w", err)
	}

	// Register tokenizers — must register all tokenizers referenced in schema
	if err := tc.RegisterTextAnalyzerRaw(tantivy.TokenizerRaw); err != nil {
		tc.Free()
		return nil, fmt.Errorf("register raw: %w", err)
	}

	if useNgram {
		if err := tc.RegisterTextAnalyzerNgram(tantivy.TokenizerNgram, 3, 5, false); err != nil {
			tc.Free()
			return nil, fmt.Errorf("register ngram: %w", err)
		}
	} else {
		if err := tc.RegisterTextAnalyzerSimple(tantivy.TokenizerSimple, 40, tantivy.English); err != nil {
			tc.Free()
			return nil, fmt.Errorf("register simple: %w", err)
		}
	}

	return &TantivyIndex{ctx: tc, schema: schema, path: indexPath, isNgram: useNgram}, nil
}

// IndexRecords indexes all records into Tantivy. Returns indexing duration.
func (t *TantivyIndex) IndexRecords(records []LogRecord, batchSize int) (time.Duration, error) {
	start := time.Now()

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		docs := make([]*tantivy.Document, 0, end-i)
		for _, r := range records[i:end] {
			doc := tantivy.NewDocument()
			if err := doc.AddField(r.DocID, t.ctx, "doc_id"); err != nil {
				return 0, fmt.Errorf("add doc_id field: %w", err)
			}
			if err := doc.AddField(r.Message, t.ctx, "message"); err != nil {
				return 0, fmt.Errorf("add message field: %w", err)
			}
			if err := doc.AddField(r.Level, t.ctx, "level"); err != nil {
				return 0, fmt.Errorf("add level field: %w", err)
			}
			if err := doc.AddField(r.Service, t.ctx, "service"); err != nil {
				return 0, fmt.Errorf("add service field: %w", err)
			}
			if err := doc.AddField(r.Host, t.ctx, "host"); err != nil {
				return 0, fmt.Errorf("add host field: %w", err)
			}
			docs = append(docs, doc)
		}

		if err := t.ctx.AddAndConsumeDocuments(docs...); err != nil {
			return 0, fmt.Errorf("add documents batch %d: %w", i/batchSize, err)
		}
	}

	return time.Since(start), nil
}

// Search runs a text search query and returns the result count and duration.
func (t *TantivyIndex) Search(query string, limit int) (count uint64, dur time.Duration, err error) {
	sCtx := tantivy.NewSearchContextBuilder().
		SetQuery(query).
		AddFieldDefaultWeight("message").
		SetDocsLimit(uintptr(limit)).
		Build()

	start := time.Now()
	result, searchErr := t.ctx.Search(sCtx)
	dur = time.Since(start)
	if searchErr != nil {
		err = fmt.Errorf("search: %w", searchErr)
		return
	}
	defer result.Free()

	count, err = result.GetSize()
	return
}

// SearchWithTagFilter runs a combined text + exact tag search.
// Uses Tantivy query parser syntax: +message:term +tagField:tagValue
func (t *TantivyIndex) SearchWithTagFilter(textQuery, tagField, tagValue string, limit int) (count uint64, dur time.Duration, err error) {
	// Build query using Tantivy query parser syntax
	query := fmt.Sprintf("+%s +%s:%s", textQuery, tagField, tagValue)

	sCtx := tantivy.NewSearchContextBuilder().
		SetQuery(query).
		AddFieldDefaultWeight("message").
		AddFieldDefaultWeight(tagField).
		SetDocsLimit(uintptr(limit)).
		Build()

	start := time.Now()
	result, searchErr := t.ctx.Search(sCtx)
	dur = time.Since(start)
	if searchErr != nil {
		err = fmt.Errorf("search: %w", searchErr)
		return
	}
	defer result.Free()

	count, err = result.GetSize()
	return
}

// SearchBoolQuery runs a boolean AND query with multiple required terms.
// Uses Tantivy query parser syntax: +term1 +term2
func (t *TantivyIndex) SearchBoolQuery(terms []string, limit int) (count uint64, dur time.Duration, err error) {
	parts := make([]string, len(terms))
	for i, term := range terms {
		parts[i] = "+" + term
	}
	query := strings.Join(parts, " ")

	sCtx := tantivy.NewSearchContextBuilder().
		SetQuery(query).
		AddFieldDefaultWeight("message").
		SetDocsLimit(uintptr(limit)).
		Build()

	start := time.Now()
	result, searchErr := t.ctx.Search(sCtx)
	dur = time.Since(start)
	if searchErr != nil {
		err = fmt.Errorf("search: %w", searchErr)
		return
	}
	defer result.Free()

	count, err = result.GetSize()
	return
}

// SearchBoolOr runs a boolean OR query with multiple optional terms.
func (t *TantivyIndex) SearchBoolOr(terms []string, limit int) (count uint64, dur time.Duration, err error) {
	query := strings.Join(terms, " OR ")

	sCtx := tantivy.NewSearchContextBuilder().
		SetQuery(query).
		AddFieldDefaultWeight("message").
		SetDocsLimit(uintptr(limit)).
		Build()

	start := time.Now()
	result, searchErr := t.ctx.Search(sCtx)
	dur = time.Since(start)
	if searchErr != nil {
		err = fmt.Errorf("search: %w", searchErr)
		return
	}
	defer result.Free()

	count, err = result.GetSize()
	return
}

// IndexSize returns the total size of the index directory in bytes.
func (t *TantivyIndex) IndexSize() int64 {
	var size int64
	filepath.Walk(t.path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		size += info.Size()
		return nil
	})
	return size
}

// Close releases the Tantivy context.
func (t *TantivyIndex) Close() {
	if t.ctx != nil {
		t.ctx.Free()
	}
}
