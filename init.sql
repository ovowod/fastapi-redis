CREATE TABLE IF NOT EXISTS users (
    id    INT          NOT NULL AUTO_INCREMENT,
    name  VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_users_email (email)
);

INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob',   'bob@example.com'),
    ('Carol', 'carol@example.com');
