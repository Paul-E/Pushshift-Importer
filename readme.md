# Pushshift data importer for reddit data

This tool takes Pushshift [data dumps](https://files.pushshift.io/reddit/) and creates a Sqlite database. Running this tool against the comments archive will create a sqlite file that enables full text search using the [FTS5](https://www.sqlite.org/fts5.html) extension of Sqlite. Posts are not currently supported.

## Requirements
 * [Rust compiler](https://www.rust-lang.org/tools/install)

## Example

The following command will scan `SOME_PATH/comments` for compressed JSON comments from Pushshift and stores them in a sqlite database named `out.db`.

    cargo run --release -- SOME_PATH/comments out.db

This will create a very large sqlite database, and may include more data than is necessary. The importer can take subreddit and username filters to limit the amount of data imported.

The command below will create a sqlite file named "out.db" that contains all comments from the /r/pushshift subreddit.

    cargo run --release -- SOME_PATH/comments out.db --subreddit pushshift

`--subreddit` and `--username` filters can be specified mulitple times, and content will be included if *any* of the filters match.

Note that username and subreddit identifiers are case sensitive. ie specifying `--subreddit PushShift` will yield and empty database.

Now you can run `sqlite3 out.db` to open that db with sqlite. Running `SELECT * FROM comment_fts WHERE body MATCH 'snoo';` in sqlite will return all comments that have the word "snoo" in it.

## Sqlite schema:
### Comment Schema

The full comment schema is available in [comment.sql](src/sqlite/comment.sql)

The comment table is defined as


    comment (id INTEGER PRIMARY KEY,
             reddit_id TEXT UNIQUE NOT NULL,
             author TEXT,
             subreddit TEXT,
             body TEXT,
             score INTEGER NOT NULL,
             created_utc INTEGER NOT NULL,
             retrieved_on INTEGER,
             parent_id TEXT NOT NULL,
             parent_is_post BOOLEAN NOT NULL);

The [FTS5](https://www.sqlite.org/fts5.html) table for comments is defined as

    comment_fts USING fts5(author, subreddit, body, content = 'comment', content_rowid = 'id')

The query `SELECT * FROM comment_fts WHERE body MATCH 'snoo'` will search all reddit comments in the database for the word "snoo".