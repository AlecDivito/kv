# Pingcap: Key Value Store (KVS)

Key value store is a command line program that is a key value store

## Command line program

The kvs executable supports the following command line arguments

```bash
kvs set <KEY> <VALUE>
# Set a key to a value. Print error and return a non-zero exit code on failure.
kvs get <KEY>
# Find a list of keys that match the provided pattern
kvs find <KEY-PATTERN>
# Get a value from the given key. Print error and return a non-zero exit code on failure.
kvs rm <KEY>
# Remove a given key. Print error and return a non-zero exit code on failure.
kvs -V
# print the version of the command line tool.
```

## Library

The kvs also contains a library to give you programmatic access to the tool.

```rust
// Restore the KvStore at a given path. Return the KvStore.
KvStore::restore(path: impl Into<PathBuf>) -> Result<KvStore>

// Set the value of a string key to a string. Return an error if the value is not written successfully.
KvStore::set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>

// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
KvStore::get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>

// Remove a given key. Return an error if the key does not exist or is not removed successfully.
KvStore::remove(&self, key: Vec<u8>) -> Result<()>

// Find a collection of given keys. Return an error if we failed to read successfully
KvStore::find(&self, like: Vec<u8>)
```

## Find pattern

The matching pattern must be given in string form. The string can have any characters
or numbers. Currently only bytes are supported. Use `_` to search for any character
and `*` to match many characters.

Examples of possible match patterns are the following:

- example
- examp_e
- ___
- exa*
- _xa*
- ___*

## Idea

You "restore" a database.

## Flexibility

Part of the feature set I want from this key value store is the ability to create
a tree of user data. It would still act as a key value store but data can be
kept as a tree and retried as such.

To accomplish this, I propose that we accept input as an _slice of strings_

eg.

```rust
KvsStore::get(&["top", "middle", "lower", "key"], "value")
```

Each level is a **family**, similar to _Column Families_ in [RocksDB](.notes/chapter-2/resources/rocksdb.md).

## TODO

| Done  | Description                                                                                                                                          |
| :---: | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
|  [x]  | maintain a log on disk (write-ahead log) of previous write commands. Evaluate that file on startup to re-create the state of the database in memory. |
|  [x]  | Extend the functionality by storing only the keys in memory, along with offsets into the on-disk log.                                                |
|  [x]  | Introduce log compaction so the log can't grow indefinitely                                                                                          |
|  [ ]  | Allow for the ability to create more "databases"                                                                                                     |
|  [ ]  | Allow for searching on keys                                                                                                                          |
|  [ ]  | Allow for defining "objects" inside of database. However, these objects are simply just bytes with partitions                                        |

## Terminology

| Term            | Description                                                            |
| --------------- | ---------------------------------------------------------------------- |
| Command         | a request made to the database                                         |
| Log             | an on-disk sequence of commands                                        |
| Log pointer     | A file offset into the log                                             |
| Log compaction  | Process of reducing the size of the database by removing stale commits |
| In-memory index | A map of keys to log pointers                                          |
| Index file      | The on-disk representation of the in-memory index                      |
