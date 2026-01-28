CREATE TABLE IF NOT EXISTS game_events
(
--     built-in fields, you can skip it (change ORDER BY if you remove event_time ;)
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
