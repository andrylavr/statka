package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Row map[string]string

type Table struct {
	Name  string `json:"name"`
	Data  []Row  `json:"data"`
	Retry int    `json:"retry"` // Счётчик попыток
}

type Storage struct {
	Tables   map[string]*Table
	retryMax int // Максимум попыток
	mu       sync.RWMutex
	db       clickhouse.Conn
}

func NewStorage() *Storage {
	client, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})

	if err != nil {
		log.Fatal("clickhouse:", err)
	}

	s := &Storage{
		Tables:   make(map[string]*Table),
		db:       client,
		retryMax: 3,
	}

	// Каждые 10 секунд сливаем в CH
	go s.flushLoop()
	return s
}

func (s *Storage) flushLoop() {
	//ticker := time.NewTicker(10 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.FlushAll()
	}
}

func (s *Storage) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for tableName, table := range s.Tables {
		if len(table.Data) == 0 {
			continue
		}

		// JSONEachRow батч
		if err := s.insertJSONEachRow(tableName, table.Data); err != nil {
			table.Retry++

			log.Printf("flush %s: %v; Retry: %d", tableName, err, table.Retry)

			if table.Retry >= s.retryMax {
				table.Data = table.Data[:0]
				table.Retry = 0
			}

			continue
		} else {
			// Очищаем после успешной записи
			table.Data = table.Data[:0]
			table.Retry = 0

			log.Printf("flushed %s: %d rows", tableName, len(table.Data))
		}
	}
}

func (s *Storage) insertJSONEachRow(tableName string, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, row := range rows {
		data, _ := json.Marshal(row)
		buf.Write(data)
		buf.WriteByte('\n')
	}

	ctx := context.Background()
	query := "INSERT INTO " + tableName + " FORMAT JSONEachRow " + buf.String()
	err := s.db.Exec(ctx, query)
	return err
}

var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

func sanitizeTableName(name string) string {
	// 1. Удаляем слеши и опасные символы
	name = strings.Trim(name, "/\\ ")

	// 2. Только буквы, цифры, подчёркивания
	if !validTableName.MatchString(name) {
		return "" // безопасный дефолт
	}

	// 3. Максимум 64 символа (ClickHouse лимит)
	if len(name) > 64 {
		name = name[:64]
	}

	return name
}

func (s *Storage) Add(tableName string, row Row) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.Tables[tableName]; !exists {
		s.Tables[tableName] = &Table{Name: tableName}
	}

	s.Tables[tableName].Data = append(s.Tables[tableName].Data, row)
}

func handler(w http.ResponseWriter, r *http.Request) {
	rawName := strings.Trim(r.URL.Path, "/")
	tableName := sanitizeTableName(rawName)

	if tableName == "" {
		http.Error(w, "invalid table name", http.StatusBadRequest)
		return
	}

	row := queryToRow(r)
	if len(row) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	storage.Add(tableName, row)

	w.Header().Set("Content-Type", "application/json")

	//w.Write([]byte("{\"ok\":1}"))

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "saved",
		"table":   tableName,
		"row_len": len(row),
		"total":   len(storage.Tables[tableName].Data),
	})
}

func queryToRow(r *http.Request) Row {
	values := r.URL.Query()
	row := make(Row, len(values))
	for k, vs := range values {
		if len(vs) > 0 {
			row[k] = vs[0]
		}
	}
	return row
}

var storage = NewStorage()

func main() {
	http.HandleFunc("/", handler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
