# Database internals

While reading the database internals book. I've come across a section on LSM Trees.
Because that is what we are trying to create here, I plan to make a note of some
of the overall designs they talk about. A lot matches whats RocksDB does and
we could probably use the spec from RocksDB for the most part, but this will help
us design our own database, which, as everyone knows, is the best use of ones time.

## LSM Trees

### New Terms

- *Read amplification*: Need to address multiple table to retrieve data
- *Write amplification*: Continuous rewrites by the compaction process
- *Space amplification*: Arising from storing multiple records associated with the same key

### MemTables

Memtables are sorted orders lists that are stored in memory.

Memtables can be flushed periodically or when a size threshold is reached. The
table is swapped with a new one before being flushed. The old table
**moves to a flushing state**. The "Flushing" table is immutable and available to
reads. While the Memtable is flushing, all records are saved in the WAL. When a
MemTable is **Flushed**, the WAL can be *trimmed*.

> In rocksDB, this would mean create a new WAL and note the current one as "achieved".

**NOTES TO SELF**: In our current implementation, we don't technically dump the
MemTable to disk, instead, we iterate through the MemTable and write it to disk.
Things like "delete" actually delete the value from the key from the table. However,
it is recommended to keep the tombstone record in the table and dump the table to
disk.

### Iterating over records

When searching for many records that match a criteria, multiple components are
opened and iterated through to a *Priority Queue (ex. min-heap)*. Each component
reads one record at a time and inserts it into the Queue. If we take Iterator 1's
item, then we ask Iterator 1 to provide a new item to take it's place. If we find
that many iterators hold the same item or the item already exists in the merged
results, we skip that result.

### Maintenance

At times, you'll need to compact multiple files into one. When doing this, we want
to create an iterator of all the files we are compacting together and read them
one item at a time.

#### Leveled compaction

Each table is separated into levels, where each level has a corresponding index
number (identifier). Higher indexes references older data.

- **Level 0**: Flushing memtables. Merge once number of tables reach a threshold. Create Level 1
- **Level 1+**: Combine data files together when they reach threshold.

When compacting memtables from level 0, the resulting files should be partitioned.
It is common to include files from a lower index when compacting files on a level.
For example, if we are compacting 3 files on level 2, with a key range from
10000-99999, we may include the files referencing that key range in level 1 at the
same time.

**As soon as the number of tables on level 1 or above reach a threshold, tables
from the current level are merged with tables on the next level holding the
overlapping key range.**

By keeping data files partitioned by key value, it's possible to skip entire
files depending on the range of data they cover.

### Sorted String Tables (SSTables)

These *Disk-resident tables* are **SSTables**. They consist of 2 components:

1. **Index file**: Allow for Logarithmic lookups (ex. b-tree) or constant time (hash tables)
2. **Data file**: What we talked about above.

Index files hold keys and data entries (offset in data file where the record is located).
New developments of *SSTable-Attached Secondary Indexes (SASI)* (implemented in
Apache Cassandra)* allow building more indexes on non-primary key fields. This
requires the index life cycle to be coupled with the SSTable life cycle. An index
is created per SSTable. **When a memtable is flushed, its contents are written
to disk, and secondary index files are created along with the SSTable primary
key index**.

Cassandra uses skiplists for secondary *index memtable implementation*.

### Bloom Filters

A probabilistic data structure for checking if a key is found in a given set of
values. Other probabilistic data structure exists, such as:

- Bloom filter: set membership
- HyperLogLog: cardinality estimation
- Count-Min Sketch: frequency estimation

Bloom filters can produce false-positive matches, but not false negative ones.
It can be used to determine if a key *might be in a table*. In LSM, each data table
and index can contain a bloom filter to determine if a given key is actually located
in a table or not.

## Un-ordered Key Value Stores

### Bitcask

All database files are log files and are written to in an unordered fashion. Rotate
the log file after it gets to a certain size. Build an in-memory hashmap called
a *keydir*, that points to the most up to date value in one of the logs. When
restoring the database, read all the log files and rebuild the *keydir* mapping.

Great for queries to get values by key. Can't or poor at doing range queries.

### WiscKey

Data is kept in *vLogs* (unordered append-only files) and keys sorted in an LSM
Tree. Keys are normally smaller and packing them into an LSM tree is cheap. This
design can be very hard to read as a lot of reads on range scans will probably use
random read I/O.

During merges (compaction), *vLogs* are read sequantially and written to a new
location. Pointers are updated to point to the new locations.

## SSDs

We will probably be writing our data out to SSDs. It's important to understand
that if we don't align our writes to the size of SSDs blocks, we could cause issues
with it that impact performance. It's better to try and write blocks to the size of
some number that is equal to the power of 2.
