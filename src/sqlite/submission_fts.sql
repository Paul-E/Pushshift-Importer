CREATE VIRTUAL TABLE IF NOT EXISTS submission_fts USING fts5(author UNINDEXED, subreddit UNINDEXED, title, selftext, content = 'submission', content_rowid = 'rowid');

CREATE TRIGGER IF NOT EXISTS submission_ai AFTER INSERT ON submission
    BEGIN
        INSERT INTO submission_fts (rowid, author, subreddit, title, selftext)
        VALUES (new.rowid, new.author, new.subreddit, new.title, new.selftext);
    END;

CREATE TRIGGER IF NOT EXISTS submission_ad AFTER DELETE ON submission
    BEGIN
        INSERT INTO submission_fts (comment_fts, author, subreddit, title, selftext)
        VALUES ('delete', old.rowid, old.author, old.subreddit, old.title, old.selftext);
    END;

CREATE TRIGGER  IF NOT EXISTS submission_au AFTER UPDATE ON submission
    BEGIN
        INSERT INTO submission_fts (comment_fts, rowid, author, subreddit, title, selftext)
        VALUES ('delete', old.rowid, old.author, old.subreddit, old.title, old.selftext);
        INSERT INTO submission_fts (rowid, author, subreddit, title, selftext)
        VALUES (new.rowid, new.author, new.subreddit, new.subreddit, new.title, new.selftext);
    END;