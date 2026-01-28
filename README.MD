# Statka — Simple Web Event Logger

**Statka** is a minimalistic event logger. Accepts events via **GET/POST** (query string, POST JSON, POST Form), buffers them in memory, and flushes batches to **ClickHouse** every N seconds.

## How It Works
Send events via HTTP to your **Statka** server:
`POST /my_another_table {"my_string": "qqq", "my_int": 123}`

Then fetch it from **ClickHouse**:
```sql
SELECT my_string
FROM my_another_table
WHERE my_int >= 123
```

## ✨ Features

- Events sent via:
  - `GET /mytable?my_string=qqq&my_int=123`
  - `POST /my_another_table {"my_string": "qqq", "my_int": 123}`
  - `POST /mytable my_string=qqq&my_int=123`
- Choose your preferred method
- **Buffering** — collects events, sends as batch
- **Retry logic** — tries again on failure, up to N attempts
- `statka.json` config — flexible settings
- **Built-in metadata**: `event_time`, `request_id`, `server_hostname`, `client_ip`, `user_agent` (auto-added if table supports)


## 🎯 Quick Start

```bash
# Start ClickHouse (skip if already running)
docker run -d -p 9000:9000 --name ch clickhouse/clickhouse-server

# Create test tables
clickhouse-client --queries-file examples/init.sql

# Run Statka
go run main.go

# Send events! (writes to game_events table)
curl "http://localhost:8080/game_events?player_id=p123&event_type=level_up&level=15"

# Check results after 5 seconds
clickhouse-client -q "SELECT * FROM game_events FORMAT PrettyCompact"
```

**Sample output:**

```
┌─event_time────────────┬─request_id─┬─server_hostname─┬─client_ip──┬─user_agent──┬─player_id─┬─event_type─┬─level─┬─weapon──────┐
│ 2026-01-31 15:33:07 │ 9e8b7ddf   │ lavr-A7         │ 127.0.0.1  │ curl/7.81.0 │ p123      │ level_up   │   15 │ ak47        │
│ 2026-01-31 15:33:07 │ 34cd8acf   │ lavr-A7         │ 127.0.0.1  │ curl/7.81.0 │ p456      │ kill       │    0 │ desert_eagle│
└──────────────────────┴────────────┴─────────────────┴────────────┴─────────────┴───────────┴────────────┴──────┴────────────┘
```

*Statka adds metadata automatically. Fields not in your table schema are ignored by ClickHouse.*

## 🗃️ **Event Table Examples**

```sql
CREATE TABLE IF NOT EXISTS game_events
(
    -- Built-in metadata fields
    event_time DateTime64(3),
    request_id String,
    server_hostname LowCardinality(String),
    client_ip String,
    user_agent String,

    -- Your custom fields
    player_id String,
    event_type LowCardinality(String),
    level UInt16,
    weapon String
)
ENGINE = MergeTree
ORDER BY event_time;

CREATE TABLE IF NOT EXISTS purchases
(
    event_time DateTime64(3) DEFAULT now(),
    player_id String,
    item_id String,
    item_name String,
    price UInt32,
    currency String
)
ENGINE = MergeTree
ORDER BY event_time;

CREATE TABLE IF NOT EXISTS web_clicks
(
    event_time DateTime64(3),
    session_id String,
    page_url String,
    user_agent String,
    referrer String,
    ip String
)
ENGINE = MergeTree
ORDER BY event_time;
```


## ✈️ **Event Sending Examples**

### 🎮 **Game Events**

```bash
# GET
curl "http://localhost:8080/game_events?player_id=p123&event_type=level_up&level=15&weapon=ak47"
curl "http://localhost:8080/game_events?player_id=p456&event_type=kill&damage=147&weapon=desert_eagle"

# POST JSON
curl -X POST "http://localhost:8080/game_events" \
  -H "Content-Type: application/json" \
  -d '{"player_id":"p789","event_type":"death","level":10}'

# POST form
curl -X POST "http://localhost:8080/game_events" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "player_id=p999&event_type=buy_skin"
```


### 🛒 **Purchases**

```bash
curl -X POST "http://localhost:8080/purchases" \
  -H "Content-Type: application/json" \
  -d '{"player_id":"p123","item_id":"skin_gold_ak47","item_name":"Golden AK-47","price":499,"currency":"USD"}'

curl "http://localhost:8080/purchases?player_id=p789&item_id=ammo_9mm&price=5¤cy=USD"
```


### 📊 **Web Analytics**

```bash
curl "http://localhost:8080/web_clicks?session_id=abc123&page_url=/shop/weapons&ip=185.23.45.67"
```


### 🌐 **Browser Events (JavaScript)**

```javascript
async function logEvent(table, data) {
    try {
        const response = await fetch(`http://localhost:8080/${table}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        const result = await response.json();
        console.log('✅ Event logged:', result); // {ok: 1}
    } catch (error) {
        console.error('❌ Send error:', error);
    }
}

// Usage examples
logEvent('game_events', {
    player_id: 'p123',
    event_type: 'level_up',
    level: 15
});

logEvent('purchases', {
    player_id: 'p456',
    item_id: 'skin_gold_ak47',
    price: 499
});
```