#include "cpppubsub.hpp"
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

int main() {
    std::cout << "Starting Worker PubSub Example...\n";

    cpppubsub::PubSub pubsub;
    cpppubsub::Worker worker;

    // Setup subscriptions
    auto sub_text = pubsub.Subscribe<std::string>("text_events");
    auto sub_nums = pubsub.Subscribe<int>("num_events");

    worker.AddSubscription<std::string>(sub_text, [](const std::string& msg) {
        std::cout << "[Worker] Processed Text Event: " << msg << "\n";
    });

    worker.AddSubscription<int>(sub_nums, [](const int& msg) {
        std::cout << "[Worker] Processed Num Event: " << msg << "\n";
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
    
    std::cout << "Finished Worker PubSub Example.\n";
    return 0;
}
