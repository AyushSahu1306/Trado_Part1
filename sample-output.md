# Sample Output and Logs

## Part 1: MQTT Data Publisher
Below are the logs showing the application connecting to the MQTT broker, subscribing to indices, calculating ATM strikes, and saving data to the database:


Database initialized

Loaded 4 topics into cache

Connected to MQTT broker

Subscribing to index: index/NIFTY

Subscribing to index: 
index/BANKNIFTY
Subscribing to index: index/FINNIFTY

Subscribing to index: index/MIDCPNIFTY

Subscribing to NIFTY options around ATM 24700

Calculated ATM for NIFTY: 24700
Saving to database: index/NIFTY, LTP: 24683.9

data saved to db

Subscribing to BANKNIFTY options around ATM 54900

Calculated ATM for BANKNIFTY: 54900
Saving to database: index/BANKNIFTY, LTP: 54877.35

data saved to db

Subscribing to FINNIFTY options around ATM 26200

Calculated ATM for FINNIFTY: 26200
Saving to database: index/FINNIFTY, LTP: 26193.85

data saved to db

Subscribing to MIDCPNIFTY options around ATM 12575

Calculated ATM for MIDCPNIFTY: 12575
Saving to database: index/MIDCPNIFTY, LTP: 12583.25
data saved to db

Inserted 4 items into ltp_data
Flushing batch to database
Database cleanup completed




### Database Query Result (Index Data)
```sql
SELECT l.id, l.topic_id, t.topic_name, t.index_name, l.ltp, l.received_at
FROM ltp_data l
JOIN topics t ON l.topic_id = t.topic_id
WHERE t.topic_name LIKE 'index%'
ORDER BY l.received_at DESC;



 id | topic_id |  topic_name   | index_name  | type | strike |   ltp    |      received_at
----+----------+---------------+-------------+------+--------+----------+-----------------------
  5 |        1 | index/NIFTY   | NIFTY       |      |        | 24690.50 | 2025-05-20 17:21:XX
  4 |        4 | index/MIDCPNIFTY | MIDCPNIFTY |      |        | 12583.25 | 2025-05-20 17:13:XX
  3 |        3 | index/FINNIFTY | FINNIFTY    |      |        | 26193.85 | 2025-05-20 17:13:XX
  2 |        2 | index/BANKNIFTY | BANKNIFTY |      |        | 54877.35 | 2025-05-20 17:13:XX
  1 |        1 | index/NIFTY   | NIFTY       |      |        | 24683.90 | 2025-05-20 17:13:XX
