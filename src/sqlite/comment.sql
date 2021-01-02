-- We need to turn on recursive triggers so that the triggers fire for "ON CONFLICT" clauses.
PRAGMA recursive_triggers = ON;
PRAGMA max_page_count = 4294967292;

-- https://kimsereylam.com/sqlite/2020/03/06/full-text-search-with-sqlite.html
CREATE TABLE IF NOT EXISTS comment (id INTEGER PRIMARY KEY,
                                    reddit_id TEXT UNIQUE NOT NULL,
                                    author TEXT,
                                    subreddit TEXT,
                                    body TEXT,
                                    score INTEGER NOT NULL,
                                    created_utc INTEGER NOT NULL,
                                    retrieved_on INTEGER,
                                    parent_id TEXT NOT NULL,
                                    parent_is_post BOOLEAN NOT NULL);

CREATE INDEX IF NOT EXISTS idx_parent_id ON comment (parent_id);
CREATE INDEX IF NOT EXISTS idx_author ON comment (author);
CREATE INDEX IF NOT EXISTS idx_subreddit ON comment (subreddit);


CREATE VIRTUAL TABLE IF NOT EXISTS comment_fts USING fts5(author, subreddit, body, content = 'comment', content_rowid = 'id');

CREATE TRIGGER IF NOT EXISTS comment_ai AFTER INSERT ON comment
    BEGIN
        INSERT INTO comment_fts (rowid, author, subreddit, body)
        VALUES (new.id, new.author, new.subreddit, new.body);
    END;

CREATE TRIGGER IF NOT EXISTS comment_ad AFTER DELETE ON comment
    BEGIN
        INSERT INTO comment_fts (comment_fts, rowid, author, subreddit, body)
        VALUES ('delete', old.id, old.author, old.subreddit, old.body);
    END;

CREATE TRIGGER  IF NOT EXISTS comment_au AFTER UPDATE ON comment
    BEGIN
        INSERT INTO comment_fts (comment_fts, rowid, author, subreddit, body)
        VALUES ('delete', old.id, old.author, old.subreddit, old.body);
        INSERT INTO comment_fts (rowid, author, subreddit, body)
        VALUES (new.id, new.author, new.subreddit, new.subreddit);
    END;