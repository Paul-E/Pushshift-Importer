CREATE TABLE IF NOT EXISTS submission (id INTEGER PRIMARY KEY,
                                       reddit_id TEXT UNIQUE NOT NULL,
                                       author TEXT,
                                       title TEXT NOT NULL,
                                       author_flair_text TEXT,
                                       subreddit TEXT NOT NULL,
                                       selftext TEXT,
                                       permalink TEXT,
                                       domain TEXT,
                                       url TEXT,
                                       score INTEGER NOT NULL,
                                       ups INTEGER,
                                       downs INTEGER,
                                       created_utc INTEGER NOT NULL,
                                       retrieved_on INTEGER,
                                       is_self BOOLEAN NOT NULL,
                                       over_18 BOOLEAN NOT NULL,
                                       spoiler BOOL,
                                       stickied BOOL,
                                       num_crossposts INTEGER);


CREATE VIRTUAL TABLE IF NOT EXISTS submission_fts USING fts5(author UNINDEXED, subreddit UNINDEXED, title, selftext, content = 'submission', content_rowid = 'id');

CREATE TRIGGER IF NOT EXISTS submission_ai AFTER INSERT ON submission
    BEGIN
        INSERT INTO submission_fts (rowid, author, subreddit, title, selftext)
        VALUES (new.id, new.author, new.subreddit, new.title, new.selftext);
    END;

CREATE TRIGGER IF NOT EXISTS submission_ad AFTER DELETE ON submission
    BEGIN
        INSERT INTO submission_fts (comment_fts, author, subreddit, title, selftext)
        VALUES ('delete', old.id, old.author, old.subreddit, old.title, old.selftext);
    END;

CREATE TRIGGER  IF NOT EXISTS submission_au AFTER UPDATE ON submission
    BEGIN
        INSERT INTO submission_fts (comment_fts, rowid, author, subreddit, title, selftext)
        VALUES ('delete', old.id, old.author, old.subreddit, old.title, old.selftext);
        INSERT INTO submission_fts (rowid, author, subreddit, title, selftext)
        VALUES (new.id, new.author, new.subreddit, new.subreddit, new.title, new.selftext);
    END;