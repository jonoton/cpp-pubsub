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

## Background Worker

For asynchronous background processing, you can use the `Worker` class. A `Worker` spawns a dedicated background thread that efficiently sleeps and wakes up immediately whenever messages arrive on any of its subscriptions.

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
