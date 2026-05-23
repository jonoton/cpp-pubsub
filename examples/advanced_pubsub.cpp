#include "cpppubsub.hpp"
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>

// Custom data structure for advanced payload
struct SensorData {
    int sensor_id;
    double temperature;
    std::string status;
};

// Thread-safe printing utility
std::mutex cout_mtx;
void print_safe(const std::string& msg) {
    std::lock_guard<std::mutex> lock(cout_mtx);
    std::cout << msg << "\n";
}

int main() {
    print_safe("Starting Advanced PubSub Example...");

    cpppubsub::PubSub pubsub;

    // 1. Configure subscription with bounded capacity and an OverflowPolicy
    // We restrict the queue size to 2, and drop the oldest messages if it overflows.
    auto sensor_sub = pubsub.Subscribe<SensorData>("sensors", 2, cpppubsub::OverflowPolicy::DropOldest);
    
    // 2. Standard subscription for text alerts
    auto alert_sub = pubsub.Subscribe<std::string>("alerts");

    std::atomic<bool> done{false};

    // 3. Using Selector to explicitly multiplex multiple topics in an event loop
    std::thread event_loop([&]() {
        cpppubsub::Selector selector;
        
        selector.Add<SensorData>(sensor_sub, [](const SensorData& data) {
            print_safe("[Event Loop] Processed Sensor " + std::to_string(data.sensor_id) + 
                       " | Temp: " + std::to_string(data.temperature) + 
                       " | Status: " + data.status);
        });

        selector.Add<std::string>(alert_sub, [](const std::string& alert) {
            print_safe("[Event Loop] ALERT: " + alert);
        });

        while (!done) {
            // WaitFor processes all available messages on registered subscribers
            selector.WaitFor(std::chrono::milliseconds(100));
        }
        
        // Final drain after completion
        selector.WaitFor(std::chrono::milliseconds(0));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Send an initial alert
    pubsub.Publish<std::string>("alerts", "Sensors Initializing...");

    // Send a rapid burst of 5 messages.
    // Because the subscriber has a capacity of 2 and policy DropOldest,
    // if the event loop doesn't read them fast enough, older ones will be dropped.
    for (int i = 1; i <= 5; ++i) {
        SensorData data = {i, 20.0 + (i * 1.5), "OK"};
        print_safe("[Publisher] Sending Sensor " + std::to_string(data.sensor_id));
        pubsub.Publish<SensorData>("sensors", data);
    }

    // Wait for the event loop to catch up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Send final alert
    pubsub.Publish<std::string>("alerts", "System Shutting Down.");
    
    // Cleanly signal termination
    done = true;
    sensor_sub->close();
    alert_sub->close();
    
    event_loop.join();

    print_safe("Finished Advanced PubSub Example.");
    return 0;
}
