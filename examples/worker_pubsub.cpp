#include "cpppubsub.hpp"
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>

// Thread-safe printing utility to avoid concurrent cout interleave issues
std::mutex cout_mtx;
void print_safe(const std::string& msg) {
    std::lock_guard<std::mutex> lock(cout_mtx);
    std::cout << msg << "\n";
}

int main() {
    print_safe("Starting Worker PubSub Example...");

    cpppubsub::PubSub pubsub;
    cpppubsub::Worker worker;

    // Setup subscriptions
    auto sub_text = pubsub.Subscribe<std::string>("text_events");
    auto sub_nums = pubsub.Subscribe<int>("num_events");

    worker.AddSubscription<std::string>(sub_text, [](const std::string& msg) {
        print_safe("[Worker] Processed Text Event: " + msg);
    });

    worker.AddSubscription<int>(sub_nums, [](const int& msg) {
        print_safe("[Worker] Processed Num Event: " + std::to_string(msg));
    });

    // Start the background worker
    worker.Start();

    // Publish some messages
    pubsub.Publish<std::string>("text_events", "Alpha");
    pubsub.Publish<int>("num_events", 42);
    pubsub.Publish<std::string>("text_events", "Beta");
    pubsub.Publish<int>("num_events", 100);

    // Wait a moment for worker to process
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    worker.Stop();
    
    print_safe("Finished Worker PubSub Example.");
    return 0;
}
