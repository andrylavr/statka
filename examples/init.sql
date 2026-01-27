CREATE TABLE game_events
(
    event_time DateTime64(3),
    event_type String, -- level_up, kill, death
    player_id  String,
    level      UInt16,
    weapon     String,
    damage     Int32
) ENGINE=MergeTree() ORDER BY event_time;

CREATE TABLE purchases
(
    event_time DateTime64(3),
    player_id  String,
    item_id    String,
    item_name  String,
    price      UInt32,
    currency   String
) ENGINE=MergeTree() ORDER BY event_time;

CREATE TABLE web_clicks
(
    event_time DateTime64(3),
    session_id String,
    page_url   String,
    user_agent String,
    referrer   String,
    ip         String
) ENGINE=MergeTree() ORDER BY event_time;
