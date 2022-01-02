CREATE VIRTUAL TABLE IF NOT EXISTS comment_fts USING fts5(author UNINDEXED, subreddit UNINDEXED, body, content = 'comment', content_rowid = 'id');

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