#include "cpppubsub.hpp"
#include <iostream>
#include <thread>
#include <string>
#include <chrono>
#include <atomic>

int main() {
    std::cout << "Starting Basic PubSub Example...\n";

    cpppubsub::PubSub pubsub;

    // Create a subscriber for a topic named "messages" of type std::string
    auto sub1 = pubsub.Subscribe<std::string>("messages");
    auto sub2 = pubsub.Subscribe<std::string>("messages");

    std::atomic<bool> publisher_done{false};
    std::mutex cout_mtx;

    auto print_safe = [&](const std::string& msg) {
        std::lock_guard<std::mutex> lock(cout_mtx);
        std::cout << msg << "\n";
    };

    std::thread publisher([&]() {
        for (int i = 0; i < 5; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::string msg = "Hello World " + std::to_string(i);
            print_safe("[Publisher] Sending: " + msg);
            pubsub.Publish("messages", msg);
        }
        // Tell subscribers we are done
        publisher_done = true;
        sub1->close();
        sub2->close();
    });

    auto subscriber_loop = [&](std::shared_ptr<cpppubsub::Subscriber<std::string>> sub, const std::string& name) {
        while (!publisher_done) {
            if (auto msg = sub->try_receive()) {
                print_safe("[" + name + "] Received: " + *msg);
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        // Drain remaining messages after publisher is done
        while (auto msg = sub->try_receive()) {
            print_safe("[" + name + "] Received: " + *msg);
        }
    };

    std::thread subscriber1([&]() { subscriber_loop(sub1, "Subscriber 1"); });
    std::thread subscriber2([&]() { subscriber_loop(sub2, "Subscriber 2"); });

    publisher.join();
    subscriber1.join();
    subscriber2.join();

    std::cout << "Finished Basic PubSub Example.\n";
    return 0;
}
