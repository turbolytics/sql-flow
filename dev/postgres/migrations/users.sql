CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    age INT CHECK (age >= 0),
    email TEXT UNIQUE NOT NULL,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    country TEXT,
    preferred_language TEXT DEFAULT 'en',
    subscription_level TEXT CHECK (subscription_level IN ('free', 'basic', 'premium')) DEFAULT 'free',
    marketing_opt_in BOOLEAN DEFAULT FALSE
);

INSERT INTO users (
    first_name, last_name, age, email, last_login, is_active,
    country, preferred_language, subscription_level, marketing_opt_in
) VALUES
    ( 'Alice', 'Smith', 30, 'alice.smith@example.com', '2025-01-15 10:15:00', TRUE, 'USA', 'en', 'premium', TRUE),
    ( 'Bob', 'Jones', 45, 'bob.jones@example.com', '2025-01-14 14:00:00', TRUE, 'Canada', 'en', 'basic', FALSE),
    ( 'Charlie', 'Brown', 28, 'charlie.brown@example.com', '2025-01-16 09:45:00', TRUE, 'UK', 'en', 'free', TRUE),
    ( 'Diana', 'Prince', 34, 'diana.prince@example.com', '2025-01-17 11:30:00', FALSE, 'USA', 'fr', 'premium', FALSE),
    ( 'Eve', 'Taylor', 29, 'eve.taylor@example.com', '2025-01-13 16:20:00', TRUE, 'Australia', 'en', 'basic', TRUE),
    ( 'Frank', 'Miller', 52, 'frank.miller@example.com', NULL, FALSE, 'Germany', 'de', 'free', FALSE),
    ( 'Grace', 'Lee', 41, 'grace.lee@example.com', '2025-01-15 08:00:00', TRUE, 'South Korea', 'ko', 'premium', TRUE),
    ( 'Hank', 'Wilson', 38, 'hank.wilson@example.com', '2025-01-18 19:00:00', TRUE, 'USA', 'en', 'basic', FALSE),
    ( 'Ivy', 'Clark', 25, 'ivy.clark@example.com', '2025-01-12 12:00:00', TRUE, 'India', 'hi', 'free', TRUE),
    ( 'Jack', 'Dawson', 36, 'jack.dawson@example.com', NULL, TRUE, 'Canada', 'en', 'premium', FALSE);
