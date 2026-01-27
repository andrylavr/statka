-- Топ убийц за час
SELECT player_id, count() as kills
FROM game_events
WHERE event_type = 'kill'
GROUP BY player_id
ORDER BY kills DESC LIMIT 10;

-- Выручка по предметам
SELECT
    item_id,
    sum(price) AS revenue,
    count()
FROM purchases
GROUP BY item_id
ORDER BY revenue DESC

-- Популярные страницы
SELECT page_url, count() as visits
FROM web_clicks
WHERE event_time > now() - INTERVAL 1 HOUR
GROUP BY page_url
ORDER BY visits DESC;
