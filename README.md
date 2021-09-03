# Pingcap: Key Value Store (KVS)

Key value store is a command line program that is a key value store

## Command line program

The kvs executable supports the following command line arguments

```bash
kvs set <KEY> <VALUE>
# Set a key to a value. Print error and return a non-zero exit code on failure.
kvs get <KEY>
# Get a value from the given key. Print error and return a non-zero exit code on failure.
kvs rm <KEY>
# Remove a given key. Print error and return a non-zero exit code on failure.
kvs -V
# print the version of the command line tool.
```

## Library

The kvs also contains a library to give you programmatic access to the tool.

```rust
// Set the value of a string key to a string. Return an error if the value is not written successfully.
KvStore::set(&mut self, key: String, value: String) -> Result<()>

// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
KvStore::get(&mut self, key: String) -> Result<Option<String>>

// Remove a given key. Return an error if the key does not exist or is not removed successfully.
KvStore::remove(&mut self, key: String) -> Result<()>

// Open the KvStore at a given path. Return the KvStore.
KvStore::open(path: impl Into<PathBuf>) -> Result<KvStore>
```

## TODO

| Done  | Description                                                                                                                                          |
| :---: | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
|  [x]  | maintain a log on disk (write-ahead log) of previous write commands. Evaluate that file on startup to re-create the state of the database in memory. |
|  [x]  | Extend the functionality by storing only the keys in memory, along with offsets into the on-disk log.                                                |
|  [x]  | Introduce log compaction so the log can't grow indefinitely                                                                                          |

## Terminology

| Term            | Description                                                            |
| --------------- | ---------------------------------------------------------------------- |
| Command         | a request made to the database                                         |
| Log             | an on-disk sequence of commands                                        |
| Log pointer     | A file offset into the log                                             |
| Log compaction  | Process of reducing the size of the database by removing stale commits |
| In-memory index | A map of keys to log pointers                                          |
| Index file      | The on-disk representation of the in-memory index                      |