---
layout: default
---

[< Previous: Getting Started](./getting-started.html) | [🏠 Home](./) | [Next: Advanced Usage >](./advanced-usage.html)
<hr>

# Basic Usage

Using `cpp-pubsub` is extremely straightforward. The central coordinator is the `cpppubsub::PubSub` broker, which manages all topics and subscriptions.

## Publishing and Subscribing

Here is a simple example demonstrating how to create a broker, subscribe to a topic, publish a message, and receive it:

```cpp
#include "cpppubsub.hpp"
#include <iostream>
#include <string>

int main() {
    // 1. Create the broker
    cpppubsub::PubSub broker;

    // 2. Subscribe to a topic (returns a shared_ptr to a Subscriber)
    auto sub = broker.Subscribe<std::string>("system_events");

    // 3. Publish a message to the topic
    broker.Publish("system_events", std::string("Initialization complete."));

    // 4. Receive messages non-blockingly
    while (auto msg = sub->try_receive()) {
        std::cout << "Received: " << *msg << std::endl;
    }

    return 0;
}
```

> **Note:** For high-frequency publishing scenarios, see [Advanced Usage: High-Performance Publishing](./advanced-usage.html#high-performance-publishing) to learn how to bypass internal broker lookups.

## Queue Capacities & Overflow Policies

**Memory Safety Built-In:** The library correctly handles C++ smart pointers (like `std::shared_ptr`). When messages are discarded due to overflow policies or when a subscriber is destroyed, the library automatically cleans up the dropped data, fully respecting reference counts and preventing memory leaks.

By default, each subscriber has an internal queue capacity of `1000` messages. If the queue fills up because the subscriber is reading too slowly, the publisher will be **blocked** until space frees up to prevent memory exhaustion.

You can customize the maximum capacity and the overflow behavior when subscribing by providing a `cpppubsub::OverflowPolicy`.

```cpp
// Allow up to 5000 messages, and drop the oldest message if the queue fills up
auto sub = broker.Subscribe<std::string>("fast_topic", 5000, cpppubsub::OverflowPolicy::DropOldest);
```

### Available Policies

- **`cpppubsub::OverflowPolicy::Block` (Default):** Pauses the publisher thread until space becomes available. Use this when you cannot afford to drop messages.
- **`cpppubsub::OverflowPolicy::DropOldest`:** Discards the oldest unread message in the subscriber's queue to make room for the new one. Use this for high-frequency topics where only the latest data matters (e.g., sensor telemetry).
- **`cpppubsub::OverflowPolicy::DropNewest`:** Discards the incoming published message if the queue is full. Use this when you want to protect the publisher but keep the oldest pending data.

---
[< Previous: Getting Started](./getting-started.html) | [🏠 Home](./) | [Next: Advanced Usage >](./advanced-usage.html)
