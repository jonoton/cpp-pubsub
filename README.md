# cpp-pubsub

`cpp-pubsub` is a fast, lightweight, and thread-safe header-only C++17 library for Publish-Subscribe messaging.

## Features
- **Header-Only:** Simply include `cpppubsub.hpp` in your project.
- **Thread-Safe by Design:** Uses standard C++ primitives (`<mutex>`, `<condition_variable>`) to guarantee safe multi-threaded dispatching. Multiple publishers and subscribers can operate simultaneously without data races.
- **Robust Memory Management & Safety:** Deep integration with C++ smart pointers guarantees memory safety. The broker safely manages `Subscriber` lifecycles and strictly respects `std::shared_ptr` reference counts to prevent memory leaks even when unread messages are dropped from full queues.
- **Move Semantics & Move-Only Types:** Native support for move-only message types (such as `std::unique_ptr<T>`) via optimized rvalue reference overloads.
- **Explicit Lifecycle Control:** Explicitly disconnect subscribers (`Unsubscribe`) or remove/clear topics (`RemoveTopic`, `ClearTopics`) when you need manual lifecycle control.
- **Background Workers & Selector:** Includes a robust, hardened `Worker` and `Selector` system for high-performance, asynchronous background multiplexing across multiple topics without spin-locking.
- **High-Performance Publishers:** Create dedicated `Publisher` objects that bypass internal map lookups and locks for maximum throughput.
- **Type-Safe Topics:** Topics and subscribers strictly enforce the type of the messages being passed, validating capacities (> 0) on creation.

## Integration

You can easily integrate this into your project via CMake's `add_subdirectory` or `FetchContent`. The target `cpppubsub::cpppubsub` is an `INTERFACE` library.

### FetchContent

```cmake
include(FetchContent)
FetchContent_Declare(
  cpppubsub
  GIT_REPOSITORY https://github.com/jonoton/cpp-pubsub.git
  GIT_TAG main
)
FetchContent_MakeAvailable(cpppubsub)

target_link_libraries(your_target PRIVATE cpppubsub::cpppubsub)
```

### Add Subdirectory

Alternatively, if you cloned the repository locally as a submodule:
```cmake
add_subdirectory(path/to/cpppubsub)

target_link_libraries(your_target PRIVATE cpppubsub::cpppubsub)
```

## Quick Start

```cpp
#include "cpppubsub.hpp"
#include <iostream>
#include <string>

int main() {
    cpppubsub::PubSub broker;

    // 1. Subscribe to a topic
    auto sub = broker.Subscribe<std::string>("system_events");

    // 2. Publish to the topic
    broker.Publish("system_events", std::string("Hello World!"));

    // 3. Receive the message
    if (auto msg = sub->try_receive()) {
        std::cout << "Received: " << *msg << std::endl;
    }

    return 0;
}
```

## Documentation
For full API documentation and advanced usage (like the `Worker` class), please refer to our [GitHub Pages Documentation Site](https://jonoton.github.io/cpp-pubsub/).

## Examples
Check out the `examples/` directory for full compilable examples demonstrating:
- Basic multi-threaded publish/subscribe (`basic_pubsub.cpp`)
- Worker multiplexing (`worker_pubsub.cpp`)
- Structured event loops and multi-topic coordination (`advanced_pubsub.cpp`)

## Building and Testing
By default, building the project will compile the examples. You can also build and run the unit tests powered by Google Test.

```bash
mkdir build && cd build
cmake .. -DCPPPUBSUB_BUILD_TESTS=ON
cmake --build .

# Run the tests
ctest --output-on-failure

# Run an example
./advanced_pubsub
```
