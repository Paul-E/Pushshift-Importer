-- https://kimsereylam.com/sqlite/2020/03/06/full-text-search-with-sqlite.html
CREATE TABLE IF NOT EXISTS comment (id INTEGER PRIMARY KEY,
                                    reddit_id TEXT UNIQUE NOT NULL,
                                    author TEXT,
                                    subreddit TEXT,
                                    body TEXT,
                                    score INTEGER,
                                    ups INTEGER,
                                    downs INTEGER,
                                    created_utc INTEGER NOT NULL,
                                    retrieved_on INTEGER,
                                    parent_id TEXT NOT NULL,
                                    parent_is_post BOOLEAN NOT NULL);
