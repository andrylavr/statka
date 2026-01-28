### Game Events
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


### Purchases

# GET
curl "http://localhost:8080/purchases?player_id=p123&item_id=skin_gold_ak47&price=499&currency=USD"

# POST JSON
curl -X POST "http://localhost:8080/purchases" \
  -H "Content-Type: application/json" \
  -d '{"player_id":"p456","item_id":"ammo_9mm","item_name":"9mm Ammo x100","price":5,"currency":"USD"}'

# POST form
curl -X POST "http://localhost:8080/purchases" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "player_id=p789&item_id=battle_pass&price=999&currency=USD"

### Web Clicks
curl "http://localhost:8080/web_clicks?session_id=abc123&page_url=/shop/weapons&ip=185.23.45.67&referrer=yandex.ru"
curl "http://localhost:8080/web_clicks?session_id=def456&page_url=/profile&ip=83.149.12.34"
