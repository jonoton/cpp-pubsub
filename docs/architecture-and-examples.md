---
layout: default
---

[< Previous: Advanced Usage](./advanced-usage.html) | [🏠 Home](./)
<hr>

# Architecture & Under the Hood

`cpp-pubsub` is explicitly designed for high performance, low latency, and zero-headache memory management across threads.

### OS-Level Events for Efficient Multiplexing
Instead of relying on CPU-heavy spinlocking or basic condition variables for `Selector` multiplexing, `cpp-pubsub` uses highly optimized OS-native handles:
- **POSIX:** Uses anonymous `pipe` and `poll`.
- **Windows:** Uses `CreateEvent` and `WaitForMultipleObjects`.

This allows the `Selector` and `Worker` to sleep natively and wake instantly when a message arrives on *any* monitored topic, ensuring minimal CPU usage while idle.

### Automatic Cleanup & Robust Memory Management
The broker tracks subscribers internally using `std::weak_ptr`. When your `Subscriber` object goes out of scope and is destroyed, the broker automatically cleans up the dead subscription during its next publish cycle. You never have to manually "unsubscribe"!

**Zero-Leak Message Queues:** `cpp-pubsub` is deeply integrated with C++ smart pointers. If you publish a `std::shared_ptr<T>` as a message, and that message is dropped from a subscriber's queue (e.g., due to the `DropOldest` policy or because the subscriber is destroyed), its reference count is automatically and safely decremented. You don't have to worry about memory leaks from unread messages!

### Strict Type Safety
Topics strictly enforce type-matching. Because `cpp-pubsub` uses template types combined with `std::dynamic_pointer_cast` internally, attempting to subscribe to an existing topic with the wrong data type will throw a `std::invalid_argument` exception.

---

# Examples & Testing

We provide complete, compilable examples in the `examples` directory of the repository to demonstrate the API in real-world scenarios:

- **`basic_pubsub.cpp`:** Demonstrates multi-threaded publish/subscribe between producer and consumer threads.
- **`advanced_pubsub.cpp`:** Demonstrates sensor systems, structured event loops, and multi-topic coordination.
- **`worker_pubsub.cpp`:** Demonstrates the `Worker` multiplexing mechanics with multiple topics.

### Building and Running the Examples

The examples are built by default. To compile and run them from the project root:

```bash
mkdir build && cd build
cmake ..
cmake --build .

# Run the basic example
./basic_pubsub

# Run the advanced example
./advanced_pubsub

# Run the worker example
./worker_pubsub
```

### Running the Unit Tests

`cpp-pubsub` includes a suite of unit tests powered by Google Test. To build and run the tests:

```bash
mkdir build && cd build
cmake .. -DCPPPUBSUB_BUILD_TESTS=ON
cmake --build .
ctest --output-on-failure
```

---
[< Previous: Advanced Usage](./advanced-usage.html) | [🏠 Home](./)
