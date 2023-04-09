# Notes

This is a document that contains all the notes for the following
course. This isn't meant to be amazing but it's meant for me to 
document any of my questions or research i've passed and want to
remember or share somewhere. If you want some context about where
this [course](https://github.com/pingcap/talent-plan/blob/master/courses/rust/docs/lesson-plan.md), it's created by pingcap to teach students about distrusted systems.

## Chapter 1

[Link to building block](https://github.com/pingcap/talent-plan/blob/master/courses/rust/building-blocks/bb-1.md)

Learning rust and commond line applications. Learn about rusts package
manager, cargo, as well how to create and test applications.

[Read More](./chapter-1/README.md)

## Chapter 2

[Link to Building block](https://github.com/pingcap/talent-plan/blob/master/courses/rust/building-blocks/bb-2.md)

This chapter focused on teaching us the use of Log structured storage
systems. It also went ahead and taught us about handling errors in
rust (which taught me a lot of new cool things i could do in the
language).

[Read More](./chapter-2/README.md)

## Chapter 3

[Link to Building block](https://github.com/pingcap/talent-plan/blob/master/courses/rust/building-blocks/bb-3.md)

This chapter focuses on refactoring the project so that it is accessible
over a server and client tool with a TCP socket. It also jumps into
comparing your solution to other production systems such as sled.
Implementation Sled inside of our database and start bench marking it.

[Read More](./chapter-3/README.md)

## Chapter 4

[Link to Building block](https://github.com/pingcap/talent-plan/blob/master/courses/rust/building-blocks/bb-4.md)

This chapter goes over building fearless concurrency using rust and goes over
some of rusts famous projects that use muilt-threading to improve performance.
Learn about `std::sync` and build your own thread pool.

[Read More](./chapter-4/README.md)

### Documentation

- [Log structure file system](chapter-2/LogStructureFileSystemPaper.md)
- [Log structure storage system](chapter-2/LogStructuredStorage.md)
- [Bitcask key/value store](chapter-2/Bitcask.md)

### Resources

- [Rust error handling](https://blog.burntsushi.net/rust-error-handling/)
  - [Defining your own error type](https://blog.burntsushi.net/rust-error-handling/#defining-your-own-error-type)
  - [Standard library traits used for error handling](https://blog.burntsushi.net/rust-error-handling/#standard-library-traits-used-for-error-handling)
- [Rust collection package](https://doc.rust-lang.org/std/collections)
  - Read for understanding when to use what
- [Rust IO package](https://doc.rust-lang.org/std/io/)
  - Read for understanding
