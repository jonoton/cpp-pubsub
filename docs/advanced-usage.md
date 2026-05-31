---
layout: default
---

[< Previous: Basic Usage](./basic-usage.html) | [🏠 Home](./) | [Next: Architecture & Examples >](./architecture-and-examples.html)
<hr>

# Advanced Usage

While `try_receive()` is great for manual polling, `cpp-pubsub` provides powerful abstractions for efficient asynchronous multiplexing over multiple topics.

## High-Performance Publishing

By default, calling `broker.Publish("topic", message)` requires a hash map lookup and a mutex lock to find the correct topic inside the broker. For high-frequency publishers, this overhead can become a bottleneck.

To bypass the broker lookup entirely, you can create a dedicated `Publisher` object. This object holds a direct reference to the underlying topic.

```cpp
// 1. Create a dedicated publisher for a specific topic
auto publisher = broker.CreatePublisher<std::string>("system_events");

// 2. Publish messages directly (no hash map lookup, no broker lock)
for (int i = 0; i < 10000; ++i) {
    publisher.Publish("Fast message " + std::to_string(i));
}
```

This approach is highly recommended when you need to publish many messages in a tight loop.

## Move-Only Types & Move Semantics

`cpp-pubsub` fully supports move-only message types (such as `std::unique_ptr<T>`) through optimized rvalue reference (`T&&`) overloads on both `Publish` and the subscriber's internal `push`.

When publishing a move-only type, ensure you use `std::move` when calling `Publish`:

```cpp
#include "cpppubsub.hpp"
#include <memory>
#include <iostream>

int main() {
    cpppubsub::PubSub broker;

    // 1. Subscribe with a move-only type
    auto sub = broker.Subscribe<std::unique_ptr<int>>("move_only_topic");

    // 2. Publish a unique_ptr by moving it
    auto ptr = std::make_unique<int>(42);
    broker.Publish("move_only_topic", std::move(ptr));

    // 3. Receive the message (takes ownership of the pointer)
    if (auto msg = sub->try_receive()) {
        std::cout << "Received value: " << **msg << std::endl;
    }

    return 0;
}
```

### Move-Only Types with `Selector` and `Worker`

Callbacks registered in `Selector::Add` and `Worker::AddSubscription` are fully templated. If your topic contains move-only types, you can define your callback to accept the parameter **by value** (e.g., `T` or `std::unique_ptr<T>`). This will automatically transfer full, exclusive ownership of the resource directly into your callback:

```cpp
auto sub = broker.Subscribe<std::unique_ptr<int>>("move_only_topic");

cpppubsub::Selector selector;
selector.Add<std::unique_ptr<int>>(sub, [](std::unique_ptr<int> ptr) {
    // We now have full ownership of the unique_ptr!
    std::cout << "Asynchronously processed pointer: " << *ptr << std::endl;
});
```

### Multi-Subscriber Distribution & Cloneable Move-Only Types

Because move-only types (like `std::unique_ptr<T>`) represent exclusive ownership of a resource, they can only be published to a **single active subscriber** by default. Attempting to publish a non-cloneable move-only type to a topic with multiple subscribers will throw a `std::runtime_error` to prevent silent resource sharing and compilation/runtime bugs.

However, if you need to broadcast a move-only type to multiple subscribers, you can make the type cloneable by providing a cloning mechanism. `cpp-pubsub` automatically detects if the type is cloneable using two approaches:

1. **Implement a `.clone() const` method:**
   If your custom class defines a `clone()` member function that returns a copy of itself, `cpp-pubsub` will automatically detect and call it.
   ```cpp
   struct CustomJob {
       int id;
       CustomJob(const CustomJob&) = delete;
       CustomJob(CustomJob&&) = default;

       CustomJob clone() const { return CustomJob{id}; }
   };
   ```

2. **Specialize the `cpppubsub::Cloner` template:**
   If you are working with third-party types (like `std::unique_ptr`) or types you cannot modify, you can specialize the `cpppubsub::Cloner` struct template inside the `cpppubsub` namespace:
   ```cpp
   namespace cpppubsub {
       template <>
       struct Cloner<SpecializedMoveOnly> {
           static SpecializedMoveOnly perform(const SpecializedMoveOnly& val) {
               return SpecializedMoveOnly{val.val};
           }
       };
   }
   ```

When multiple subscribers are present, `cpp-pubsub` uses the `Cloner` mechanism to perform copies/clones for all but the last subscriber, and performs a zero-overhead **direct move** into the final subscriber, optimizing both safety and performance.


## Background Worker

For asynchronous background processing, you can use the `Worker` class. A `Worker` spawns a dedicated background thread that efficiently sleeps and wakes up immediately whenever messages arrive on any of its subscriptions.

**Note for Windows Users:** For optimal performance, it is highly recommended to limit each `Worker` to a maximum of 64 subscriptions. Exceeding this limit invokes the Windows Thread Pool API fallback, which carries higher latency. If you have hundreds of subscriptions, distribute them across multiple `Worker` instances.

```cpp
#include "cpppubsub.hpp"
#include <iostream>
#include <string>

int main() {
    cpppubsub::PubSub broker;
    auto sub = broker.Subscribe<std::string>("system_events");

    // Initialize a Worker
    cpppubsub::Worker worker;

    // Add subscriptions and their associated callbacks
    worker.AddSubscription<std::string>(sub, [](const std::string& msg) {
        std::cout << "[Background] Processed: " << msg << "\n";
    });

    // Optional: Set a recurring tick callback (e.g., runs every 500ms)
    // This allows the worker to perform periodic background tasks when idle.
    worker.SetTickCallback(std::chrono::milliseconds(500), []() {
        std::cout << "[Background] Worker tick (idle)...\n";
    });

    // Start the background thread
    worker.Start();

    // Publish messages from the main thread
    broker.Publish("system_events", std::string("Hello from Main!"));

    // ...

    // Stop blocks until the background thread cleanly exits
    worker.Stop();
    return 0;
}
```

## Manual Multiplexing with `Selector`

If you already have your own event loop or thread and do not want to spawn a new one with `Worker`, you can manually multiplex multiple subscribers natively using the `Selector` class.

The `Selector` is highly optimized: it does not use a spin-lock. Instead, it waits on native OS events until a message arrives.

> [!WARNING]
> **Callback Lifetimes:** Callbacks registered via `Selector::Add` are captured by value. Ensure that any objects referenced by the callback outlive the `Selector` or the corresponding `Subscriber` to avoid dangling references and undefined behavior.

> [!NOTE]
> **Safety & Robustness Built-in:**
> - **No Deadlocks or Busy Loops:** The `Selector` automatically pre-checks and cleans up expired subscribers, avoiding deadlocks or CPU busy loops if a monitored subscriber is destroyed. On Linux/macOS, it cleanly handles `POLLERR`, `POLLHUP`, and `POLLNVAL` events.
> - **Fair Polling:** On Windows, it safely handles multiple concurrent event signals fairly to prevent starving other subscribers, and implements proper cleanup if fallback thread pool registration fails.
> - **Thread-Safe Workers:** The `Worker` background class is fully thread-safe. Changes to timeouts or tick callbacks (via `SetTickCallback`) are guarded by mutexes to prevent concurrent read/write race conditions.
> - **Timeout Rounding & Clamping:** Small sub-millisecond positive wait timeouts are rounded up to `1ms` to prevent zero-timeout high-CPU busy-wait loops. Additionally, extremely large timeouts (e.g. `std::chrono::hours::max()`) are safely clamped to prevent duration cast and integer overflows.

**Note for Windows Users:** Just like `Worker`, a `Selector` on Windows performs optimally when tracking 64 or fewer subscriptions. Exceeding 64 subscriptions triggers a Thread Pool API fallback to bypass the `WaitForMultipleObjects` limit, which introduces higher overhead.

```cpp
std::atomic<bool> keep_running{true};

std::thread custom_worker([&]() {
    cpppubsub::Selector selector;
    
    selector.Add<std::string>(sub1, [](const std::string& msg) {
        std::cout << "Sub 1: " << msg << "\n";
    });
    selector.Add<int>(sub2, [](const int& msg) {
        std::cout << "Sub 2: " << msg << "\n";
    });

    while (keep_running) {
        // Blocks for up to 100ms waiting for messages on ANY subscriber.
        // If a message arrives, the associated callback is executed.
        // Returns true if any events were processed, false if it timed out.
        bool processed = selector.WaitFor(std::chrono::milliseconds(100));

        if (!processed) {
            // No messages arrived within 100ms.
            // You can safely do other background work here!
        }
    }
});

// ... later, when shutting down ...
keep_running = false;
custom_worker.join();
```

---
[< Previous: Basic Usage](./basic-usage.html) | [🏠 Home](./) | [Next: Architecture & Examples >](./architecture-and-examples.html)
