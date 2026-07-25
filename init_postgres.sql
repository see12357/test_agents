-- Initialization script for mock-postgres test database
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial test records
INSERT INTO users (email)
SELECT 'user_' || g || '@example.com'
FROM generate_series(1, 100) AS g;

INSERT INTO transactions (user_id, amount, status)
SELECT 
    (g % 100) + 1,
    (g * 12.5),
    CASE WHEN g % 2 = 0 THEN 'completed' ELSE 'pending' END
FROM generate_series(1, 500) AS g;

ANALYZE users;
ANALYZE transactions;
