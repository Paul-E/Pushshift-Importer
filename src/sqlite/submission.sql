CREATE TABLE IF NOT EXISTS submission (reddit_id TEXT UNIQUE NOT NULL,
                                       author TEXT,
                                       title TEXT NOT NULL,
                                       author_flair_text TEXT,
                                       link_flair_text TEXT,
                                       subreddit TEXT,
                                       selftext TEXT,
                                       permalink TEXT,
                                       domain TEXT,
                                       url TEXT,
                                       score INTEGER,
                                       ups INTEGER,
                                       downs INTEGER,
                                       created_utc INTEGER NOT NULL,
                                       retrieved_on INTEGER,
                                       is_self BOOLEAN NOT NULL,
                                       over_18 BOOLEAN NOT NULL,
                                       spoiler BOOLEAN,
                                       pinned BOOLEAN,
                                       stickied BOOLEAN NOT NULL,
                                       num_crossposts INTEGER);