# SQLite

SQLite is the default backend: `dally server` with no configuration stores
everything in `./dally.sqlite`. It needs no database server, which makes it
the right choice for local development, tests, and small deployments that run
on a single machine.

```toml
[storage]
url = "sqlite://dally.sqlite"          # relative to the working directory
# url = "sqlite:///var/lib/dally/dally.sqlite"   # absolute path
# url = "sqlite::memory:"              # gone when the process exits
```

The file is created if it does not exist, and migrations run on startup.

## One server process

Only one `dally server` can use a database file at a time. dally takes an
exclusive lock on `dally.sqlite.lock` next to the database, and a second
server on the same file refuses to start:

```
SQLite database dally.sqlite is already in use by another dally server
```

The lock is released when the process exits, even if it crashes, so a stale
lock file never blocks a restart.

The reason is that long polls are woken through an in-process channel, and
timestamps come from the process clock. A second process would not wake the
first one's waiting workers, and the two clocks need not agree. If you need
more than one server, use PostgreSQL.

You can still have any number of *workers*: they talk to the server over
HTTP, not to the file.

## Guarantees

- Writes are durable once the request returns. The database runs in WAL mode,
  which keeps reads from waiting on writes.
- Leasing takes SQLite's write lock before it looks for due messages, so two
  concurrent polls never get the same message.
- All writes go through one connection, so throughput is bounded by a single
  writer. That is thousands of small operations per second on an SSD, which is
  plenty for most single-machine setups.

## Backups

Copy the database while the server runs with the `sqlite3` shell, which takes
a consistent snapshot:

```sh
sqlite3 dally.sqlite ".backup dally-backup.sqlite"
```

Copying the file with `cp` while the server is running can produce a corrupt
copy, because the `-wal` file holds recent writes.

## When to move to PostgreSQL

- You want more than one dally server, for availability or throughput.
- The database lives on a network file system. SQLite's locking is unreliable
  there.
- You already run PostgreSQL and would rather back up one database than two.
