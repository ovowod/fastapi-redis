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

CREATE TABLE IF NOT EXISTS articles (
    id         INT          NOT NULL AUTO_INCREMENT,
    title      VARCHAR(255) NOT NULL,
    view_count INT          NOT NULL DEFAULT 0,
    like_count INT          NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

INSERT INTO articles (title) VALUES
    ('Redis 캐시 전략 정리'),
    ('Write-Back vs Write-Through'),
    ('FastAPI 비동기 처리');

CREATE TABLE IF NOT EXISTS article_likes (
    user_id    INT NOT NULL,
    article_id INT NOT NULL,
    PRIMARY KEY (user_id, article_id),
    FOREIGN KEY (user_id)    REFERENCES users(id),
    FOREIGN KEY (article_id) REFERENCES articles(id)
);
