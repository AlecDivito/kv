# RocksDB: A persistent key-value store for fast storage environments

This document is a small summary of the following product:
[RocksDB: A persistent key-value store for fast storage environments](https://rocksdb.org/)

## WAL Overview

Every update causes writes to happen in 2 locations:

1. MemTable (in memory btree)
2. Write ahead log(WAL) on disk

## MemTable

Hold data in memory before it's flushed to a SST. It must be queried for data first
as it holds the most recent data. Once full, a background thread will flush it
into a `.sst` file. The default size of a MemTable is 64MB. Note that if you are
using families, you can also set a max MemTable size across all of the family mem tables.

The data structure used in MemTables is a skip list.

MemTables will write when:

1. A table size exceeds the buffer size after a write (default 64MB)
2. Total size of all mem tables exceeds whats set, flush the largest table
3. Max WAL size is hit. In this scenario ?? the memtable with the oldest data will be flushed ??

A better explanation of #3.

The oldest MemTable will be flushed to disk. The MemTable will become a SST file.
A new WAL is create and all future writes happen here. The old WAL file won't be
written to anymore but the deletion will be delayed.

The Old WAL file **won't** be removed until **ALL** MemTable families have been
flushed to disk! That means it's possible to have **MANY** WAL if the database
has been open for a long time with many MemTables.

## Journaling

Two types: 1. WAL and 2. Manifest

### WAL

A WAL is created when:

1. A new database is opened
2. a colum family is flushed

A WAL can be deleted once all column families have been flushed to SST files. Archived
files are stored somewhere in an archive folder.

WAL's are managed in a directory. Each new WAL is an increasing sequence number.
The database is restored by reading the WAL's in order.

A WAL consists of 32K blocks. Each block can contain many records. The reader and
writer read these blocks one at a time. Each record consists of the following:

1. CRC (4 bytes)
2. Size (2 bytes)
3. Type of Record (1 byte) (used mainly for records larger then 32K block)
4. Payload (N bytes)

```text
block := record* trailer?
record :=
  checksum: uint32	// crc32c of type and data[]
  length: uint16
  type: uint8		// One of FULL, FIRST, MIDDLE, LAST 
  data: uint8[length]
```

Be careful about **Write Amplification**. The filesystem will always communicate
in 4k-8k blocks, so even if you are only writing 40 bytes to disk, you may still
be sending a 4-8k block of data.

The lifecycle of a WAL is:

1. Creation
2. Closing: the WAL file is completed and closed
3. Obsoletion
   1. When column family's in a WAL have all been flushed to disk

When a WAL is created, write the log number to MANIFEST to indicate the new WAL
is created. Track the closing events by:

- closing and syncing the WAL. Write log number and CRC of WAL to manifest to indicate it as complete
- if closed but not synced, don't save to manifest file and allow recovery to look up the WAL number.

When a MemTable is flushed, sync Manifest file

### Manifest

Journal the in-memory state **updates**. Keep track of RocksDB state changes in
a transactional log. It is made up of a **CURRENT** file which points to the most
recent Manifest file. RocksDB state is stored in many **MANIFEST-<SEQ-NO>** which
is a log. Any state change is records to a Manifest file. When it grows to large,
a new file is created. If a new state change creates a new manifest file, the current
file is updated to point at that new file.

Everytime there is a change, a new version of the MANIFEST is written to the MANIFEST
log. The types of data stored in manfest file are:

- Latest WAL log file number
- Previous manifest file number
- Next file number edit record
- Last sequence number
- Max amount of family columns allowed
- Marking file as "deleted" from database
- Newly Added files to database

Files stored in the manfiest file, includes metadata like:

- file type
- level
- file number
- file size
- file location
- smallest key
- largest key
- smallest_seq number
- largest_seq number

### MANIFEST in WAL

It's recommend to write the MANIFEST updates to the WAL. That way it's stored in
2 locations.

## Partitioning

Rocks DB allows partitioning data using **ColumnFamilies**. You can provide this
and it will be used as a "prefix" ahead of all of your records. When you hold a
reference to a "ColumnFamily" you are still writing to the WAL however, you will
reference the ColumnFamily you are writing to as a number.

The main idea is that these families **share the WAL, but not the Memtable**. We
can only delete the WAL, once all Column Families have been flushed and all data
contained in the WAL is saved into table files.