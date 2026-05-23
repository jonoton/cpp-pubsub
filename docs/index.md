---
layout: default
---

# cpp-pubsub

`cpp-pubsub` is a fast, header-only C++ library for thread-safe Publish/Subscribe messaging. It is designed to be lightweight and simple to integrate into any modern C++17 codebase.

### Key Features
- **Thread-Safe by Design:** Safe multi-threaded dispatching with standard C++ primitives ensures publishers and subscribers can interact without data races.
- **Robust Memory Management:** Built from the ground up to respect C++ smart pointers (`std::shared_ptr`). Zero-leak guarantees even when unread messages are dropped from full queues, and automatic cleanup of disconnected subscribers.
- **Background Workers:** OS-level events power high-performance background multiplexing for advanced systems.

## Documentation Pages

Welcome to the `cpp-pubsub` documentation! Please follow the guide below to learn how to integrate and use the library:

1. **[Getting Started](./getting-started.html)**
   Learn how to integrate `cpp-pubsub` into your project via CMake or direct inclusion.
   
2. **[Basic Usage](./basic-usage.html)**
   Learn the core concepts of publishing, subscribing, and customizing queue overflow policies.

3. **[Advanced Usage](./advanced-usage.html)**
   Discover how to multiplex multiple topics efficiently using the `Worker` and `Selector` classes.

4. **[Architecture & Examples](./architecture-and-examples.html)**
   Understand the OS-level primitives powering the library, how to build the included examples, and how to run unit tests.

---
[Start Reading: Getting Started >](./getting-started.html)
