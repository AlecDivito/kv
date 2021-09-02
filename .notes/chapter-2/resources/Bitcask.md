# Bitcask: A log-Structured Hash Table for Fast Key/Value Data

This document is a small summary of the following paper:
[A log-Structured Hash Table for Fast Key/Value Data](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)

Bitcask repository is hosted on github. To read it go [here](https://github.com/basho/bitcask)

## Abstract

Bitcask is a database Server that writes to files. Where the
processes is killed or stopped, files become immutable and can only
be read from. Bitcask only writes to an `active file` in an append
only fashion, therefore no disk seeks need to occur.

## Log Structure

Each new key/value is stored inside of an append only file in the
following format:

1. `crc`: Cyclic Redundancy check (hash)
2. `tstamp`: 32-bit int local timestamp for internal use
3. `ksz`: size of the key (in bytes)
4. `value_sz`: size of the value (in bytes)
5. `key`: actual key
6. `value`: actual value

## Process

After a user inserts a new key and it's appended to disk, an
in-memory hashmap is updated. This maps every key to a fixed-size
structure giving the file, offset, and size of the most recently
written entry for the key.

## In-memory hashmap structure

Each new addition to the file system includes a in-memory hashmap
that is used to keep track of where the keys value is stored on disk.
The maps structure looks like so:

1. `Key`: Actual key used by user
   1. `file_id`: What file are we referencing
   2. `value_sz`: Whats the size of the value (in bytes)
   3. `value_pos`: Where does the value start on disk
   4. `tstamp`: 32-bit int local timestamp for internal use

## Cleaning up (Merging)

Constantly writing new values to new files can be wasteful. To solve
this we can merge all non-active files together into a new file thus
containing all of the latest key value pairs. Once merged, another
file called the `hint file` will be generated. The `hint file` are
essentially like data files but instead of values, they contain
the position and size of the values within the corresponding data
file.
