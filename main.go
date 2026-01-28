package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	ClickHouse struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Username string `json:"username"`
		Password string `json:"password"`
		Database string `json:"database"`
	} `json:"clickhouse"`
	Server struct {
		Port int `json:"port"`
	} `json:"server"`
	Storage struct {
		FlushInterval int `json:"flush_interval"`
		RetryMax      int `json:"retry_max"`
	} `json:"storage"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

type Row map[string]interface{}

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

func NewStorage(cfg *Config) *Storage {
	client, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.ClickHouse.Host, cfg.ClickHouse.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouse.Database,
			Username: cfg.ClickHouse.Username,
			Password: cfg.ClickHouse.Password,
		},
	})

	if err != nil {
		log.Fatal("clickhouse:", err)
	}

	s := &Storage{
		Tables:   make(map[string]*Table),
		db:       client,
		retryMax: cfg.Storage.RetryMax,
	}

	// Каждые 10 секунд сливаем в CH
	go s.flushLoop(cfg.Storage.FlushInterval)
	return s
}

func (s *Storage) flushLoop(seconds int) {
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)
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

			log.Printf("Flush %s: %v; Retry: %d", tableName, err, table.Retry)

			if table.Retry >= s.retryMax {
				log.Printf("%s DROPPED after %d retries", tableName, s.retryMax)
				delete(s.Tables, tableName)
			}
		} else {
			log.Printf("flushed %s: %d rows", tableName, len(table.Data))

			// Очищаем после успешной записи
			table.Data = table.Data[:0]
			table.Retry = 0
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

func realIP(r *http.Request) string {
	// X-Forwarded-For, X-Real-IP → реальный IP прокси
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return strings.Split(ip, ",")[0]
	}
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return ip
	}
	return strings.Split(r.RemoteAddr, ":")[0] // fallback
}

var serverHostname string

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		serverHostname = "unknown"
	} else {
		serverHostname = hostname // statka-01, worker-nyc-03, etc.
	}
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

	row := parseRow(r)
	if len(row) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	//built-in data
	row["event_time"] = fmt.Sprintf("%d", time.Now().UTC().UnixMilli())
	row["request_id"] = uuid.New().String()[:8]
	row["server_hostname"] = serverHostname
	row["client_ip"] = realIP(r)
	row["user_agent"] = r.UserAgent()

	storage.Add(tableName, row)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{\"ok\":1}"))
}

func formToRow(form url.Values) Row {
	row := make(Row)
	for k, vs := range form {
		if len(vs) > 0 {
			row[k] = vs[0]
		}
	}
	return row
}

func parsePost(r *http.Request) Row {
	contentType := r.Header.Get("Content-Type")
	switch {
	case strings.Contains(contentType, "application/json"):
		var row Row
		if json.NewDecoder(r.Body).Decode(&row) == nil {
			return row
		}
	default:
		if err := r.ParseForm(); err == nil {
			return formToRow(r.Form)
		}
	}
	return nil
}

func parseGet(r *http.Request) Row {
	values := r.URL.Query()
	row := make(Row, len(values))
	for k, vs := range values {
		if len(vs) > 0 {
			row[k] = vs[0]
		}
	}
	return row
}

func parseRow(r *http.Request) Row {
	switch r.Method {
	case http.MethodPost:
		return parsePost(r)
	case http.MethodGet:
		return parseGet(r)
	default:
		return nil
	}
}

var storage *Storage

func main() {
	cfg, err := LoadConfig("statka.json")
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	storage = NewStorage(cfg)

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("Graceful shutdown...")
		storage.FlushAll()
		os.Exit(0)
	}()

	http.HandleFunc("/", handler)

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	log.Printf("Statka ready! Listening on http://localhost%s", addr)
	log.Printf("Flush every %d sec, retry %d times", cfg.Storage.FlushInterval, cfg.Storage.RetryMax)
	log.Fatal(http.ListenAndServe(addr, nil))
}
