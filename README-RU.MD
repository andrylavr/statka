# Statka — Очень простой логгер событий для Web

**Statka** — минималистичный логгер.
Принимает события через **GET / POST** (query string, POST JSON, POST Form), буферизирует в памяти и каждые N секунд сливает батчами в **ClickHouse**.

## Как это работает
Отправляйте события по HTTP на свой сервер в **Statka**:
`POST /my_another_table {"my_string": "qqq", "my_int": 123}`

И забирайте из **ClickHouse** так:
```sql
SELECT my_string
FROM my_another_table
WHERE my_int >= 123
```

## 🛠️ Установка
```shell
git clone https://github.com/andrylavr/statka
cd statka
make
sudo make install
make clean
systemctl status statka
```

✅ После установки сервис будет доступен на http://localhost:8080! 🚀
Вы всегда можете изменить порт в конфиге `/opt/statka/statka.json`

## ⚙️ Изменение конфига после установки
```shell
sudo nano /opt/statka/statka.json
sudo systemctl restart statka
```

## ✨ Особенности

- События отправляются с помощью
  - `GET /mytable?my_string=qqq&my_int=123`
  - `POST /my_another_table {"my_string": "qqq", "my_int": 123}`
  - `POST /mytable my_string=qqq&my_int=123`
- Вы сами выбираете, какой способ Вам подходит
- Буферизация — копит события, затем отсылает батчем
- Retry логика — если не получилось, попробовать снова позже, но не более N-раз
- Конфиг `statka.json` — гибкие настройки
- Встроенные метаданные: `event_time`, `request_id`, `server_hostname`, `client_ip`, `user_agent`. Добавляется автоматически, если есть в принимающей таблице 

## 🎯 Быстрый старт

```bash

# Стартуем ClickHouse в докере
# Если у Вас уже установлен и запущен ClickHouse, то этот шаг можно пропустить
docker run -d -p 9000:9000 --name ch clickhouse/clickhouse-server

# Создаём тестовые таблицы
clickhouse-client --queries-file examples/init.sql

# Запуск Statka
go run main.go

# Присылаем события! 
# (здесь события пишутся в таблицу game_events
# Можно указать любую таблицу которая есть в вашей БД в ClickHouse)
curl "http://localhost:8080/game_events?player_id=p123\&event_type=level_up\&level=15"

# Через 5 сек проверяем что всё пришло в ClickHouse
clickhouse-client -q "SELECT * FROM game_events FORMAT PrettyCompact"

#    ┌──────────────event_time─┬─request_id─┬─server_hostname─┬─client_ip─┬─user_agent──┬─player_id─┬─event_type─┬─level─┬─weapon───────┐
# 1. │ 2026-01-31 15:33:07.704 │ 9e8b7ddf   │ lavr-A7         │ 127.0.0.1 │ curl/7.81.0 │ p123      │ level_up   │    15 │ ak47         │
# 2. │ 2026-01-31 15:33:07.709 │ 34cd8acf   │ lavr-A7         │ 127.0.0.1 │ curl/7.81.0 │ p456      │ kill       │     0 │ desert_eagle │
# 3. │ 2026-01-31 15:33:07.713 │ 115b6190   │ lavr-A7         │ 127.0.0.1 │ curl/7.81.0 │ p789      │ death      │    10 │              │
# 4. │ 2026-01-31 15:33:07.718 │ 380eb607   │ lavr-A7         │ 127.0.0.1 │ curl/7.81.0 │ p999      │ buy_skin   │     0 │              │
#    └─────────────────────────┴────────────┴─────────────────┴───────────┴─────────────┴───────────┴────────────┴───────┴──────────────┘
#
# Как видно из таблицы, Statka присылает дополнительные данные вдобавок к тем, что у Вас есть.
# Если эти столбцы с дополнительными данными не создавать в таблице, то они не будут сохраняться и отображаться
```

## Примеры создания таблиц для событий
```sql
CREATE TABLE IF NOT EXISTS game_events
(
--     built-in fields
    event_time DateTime64(3),
    request_id String,
    server_hostname LowCardinality(String),
    client_ip String,
    user_agent String,

-- user defined custom fields
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
    player_id  String,
    item_id    String,
    item_name  String,
    price      UInt32,
    currency   String
)
ENGINE = MergeTree
ORDER BY event_time;

CREATE TABLE IF NOT EXISTS web_clicks
(
    event_time DateTime64(3),
    session_id String,
    page_url   String,
    user_agent String,
    referrer   String,
    ip         String
)
ENGINE = MergeTree
ORDER BY event_time;
```

## 📨 Примеры отправки событий
### 🎮 Игровые события
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

### 🛒 Покупки

```bash
curl -X POST "http://localhost:8080/purchases" \
-H "Content-Type: application/json" \
-d '{"player_id":"p123","item_id":"skin_gold_ak47","item_name":"Golden AK-47","price":499,"currency":"USD"}'

curl "http://localhost:8080/purchases?player_id=p789\&item_id=ammo_9mm\&price=5¤cy=USD"
```

### 👀 Веб аналитика

```bash
curl "http://localhost:8080/web_clicks?session_id=abc123\&page_url=/shop/weapons\&ip=185.23.45.67"
```

### 🌍 Отправляем события из браузера
```js

async function logEvent(table, data) {
    try {
        const response = await fetch(`http://localhost:8080/${table}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        const result = await response.json();
        console.log('✅ Event logged:', result);  // {ok: 1}
    } catch (error) {
        console.error('❌ Ошибка отправки:', error);
    }
}

// 🎮 Примеры использования
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