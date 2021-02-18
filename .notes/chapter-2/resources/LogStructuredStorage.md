# Damn Cool Algorithms: Log structured storage

This document is a small summary of the following article:
[Damn cool Algorithms: Log structed storage](http://blog.notdot.net/2009/12/Damn-Cool-Algorithms-Log-structured-storage)

## What is it

Basically a file inside of a file system that is append only sequence of data
entries. When adding new elements, just append the sequence of bytes to the end
of the log.