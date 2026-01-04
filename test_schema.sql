-- Schema for testing custom types
CREATE TABLE IF NOT EXISTS type_tests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bool_val INTEGER,
    float_val REAL,
    int_val INTEGER,
    string_val TEXT,
    time_val DATETIME,
    json_val TEXT,
    json_raw_val TEXT
);

-- Insert test data
INSERT INTO type_tests (bool_val, float_val, int_val, string_val, time_val, json_val, json_raw_val)
VALUES (1, 3.14, 42, 'hello', '2024-01-15 10:30:00', '{"key": "value"}', '{"raw": true}');

INSERT INTO type_tests (bool_val, float_val, int_val, string_val, time_val, json_val, json_raw_val)
VALUES (0, 2.718, 100, 'world', '2024-12-31 23:59:59', '{"name": "test", "count": 5}', '[1, 2, 3]');

-- Insert row with NULL values
INSERT INTO type_tests (bool_val, float_val, int_val, string_val, time_val, json_val, json_raw_val)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL);
