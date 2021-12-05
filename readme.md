# Pushshift data importer for reddit data

This tool takes Pushshift [data dumps](https://files.pushshift.io/reddit/) and creates a Sqlite database. Running this tool against the comments archive will create a sqlite file that enables full text search using the [FTS5](https://www.sqlite.org/fts5.html) extension of Sqlite.

## Requirements
 * [Rust compiler](https://www.rust-lang.org/tools/install)
 * Data from Pushshift [data dumps](https://files.pushshift.io/reddit/) (or files in a compatible JSON format)

## Running the importer
### Quick start

The following command will scan `SOME_PATH/comments` for compressed JSON comments `SOME_PATH/submissions` for compressed
JSON comments and stores them in a sqlite database named `out.db`. 

    cargo run --release -- --comments SOME_PATH/comments --submissions SOME_PATH/submissions SOME_PATH/out.db

The input comments files, submissions files, and output file should be located in different directories. The input file
format is specified by the JSON files that exists in the Pushshift data dump.

### Filtering

Running the command above will create a very large sqlite database, and may include more data than is necessary.
The importer can take subreddit and username filters to limit the amount of data imported.

The command below will create a sqlite file named "out.db" that contains all submissions and comments from
the /r/pushshift subreddit.

    cargo run --release -- --comments SOME_PATH/comments --submissions SOME_PATH/submissions SOME_PATH/out.db --subreddit pushshift

`--subreddit` and `--username` filters can be specified multiple times, and content will be included if *any* of the
subreddit or username filters match.

Adding another filter such as `min-score` as below

    cargo run --release -- --comments SOME_PATH/comments --submissions SOME_PATH/submissions SOME_PATH/out.db --subreddit pushshift --min-score 10

will restrict the import to the same content as above, with an additional requirement that 

Note: The username and subreddit identifiers are case sensitive. ie specifying `--subreddit PushShift` will yield and
empty database (because the subreddit is names `pushshift`).

#### Available filters:

* `subreddit` - Include comments and submissions from this subreddit. May be specified multiple times, and
items will be included if they match any subreddit filter
* `user` - Include comments and submissions from this user. May be specified multiple times, and
items will be included if they match any user filter
* `min-score` & `max-score` - Only include content with score equal to or between these values. Content without a score is always included 
* `min-datetime` & `max-datetime` - Only include content posted on or between these dates.
   The date format is `%Y-%m-%d-%H:%M:%S`, eg `2015-09-05-23:56:04`. Time is assumed to be UTC. To avoid time zone issues,
   it is probably easiest to add a day on each side of your desired interval.
  
The user and subreddit filters are ORed against each other. If the content matches *either* the subreddit filter
*or* the user filter, the content will be included if the other filter criteria is also satisfied.

## Your database
Once the importer has run and succesfully completed you can run `sqlite3 out.db` to open that db with sqlite.
Running `SELECT * FROM comment_fts WHERE body MATCH 'snoo';` in sqlite will return all comments that have the word "snoo" in it.
Enter `.schema` into the sqlite3 command line to see the table format.

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

### Submission Schema

The submission table has the following schema

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

With the FTS schema being defined by:

    CREATE VIRTUAL TABLE IF NOT EXISTS submission_fts USING fts5(author UNINDEXED, subreddit UNINDEXED, title, selftext, content = 'submission', content_rowid = 'id');
