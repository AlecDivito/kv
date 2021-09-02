# The Design and Implementation of a Log-Structured File System

This document is a small summary of the following paper:
[The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)

## Abstract

Creating a system that can quick write and recover data by using a
`log-structured file system`. The log is structured on disk and includes
indexing information. Logs are separated into segments and use a segment
cleaner to removed old data and compress the logs.

## Idea

The fundamental idea of a log-structured file system is to improve write
performance by buffering a sequence of file system changes in the file cache
and then writing all the changes to disk sequentially in a single disk write
operation. The information written to disk in the write operation includes
file data blocks, attributes, index blocks, directories, and almost all the
other information used to manage the file system. For workloads that contain
many small files, a log-structured file system converts the many small
synchronous random writes of traditional file systems into large asynchronous
sequential transfers that can utilize nearly 100% of the raw disk bandwidth.

## 2 main issues

1. How to retrieve information from the log
2. How to manage free space on disk (make sure large areas of disk are available)

## How it works