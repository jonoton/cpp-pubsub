#include <gtest/gtest.h>
#include "cpppubsub.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>

using namespace cpppubsub;

// Test Basic Publishing and Subscribing
TEST(PubSubTest, BasicDispatch) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("test_topic");
    
    ps.Publish<int>("test_topic", 42);
    
    auto msg = sub->try_receive();
    ASSERT_TRUE(msg.has_value());
    EXPECT_EQ(msg.value(), 42);
    
    // Queue should be empty now
    auto msg2 = sub->try_receive();
    EXPECT_FALSE(msg2.has_value());
}

// Test Multiple Subscribers on the same topic
TEST(PubSubTest, MultipleSubscribers) {
    PubSub ps;
    auto sub1 = ps.Subscribe<std::string>("multi_topic");
    auto sub2 = ps.Subscribe<std::string>("multi_topic");
    
    ps.Publish<std::string>("multi_topic", "hello");
    
    auto msg1 = sub1->try_receive();
    auto msg2 = sub2->try_receive();
    
    ASSERT_TRUE(msg1.has_value());
    ASSERT_TRUE(msg2.has_value());
    EXPECT_EQ(msg1.value(), "hello");
    EXPECT_EQ(msg2.value(), "hello");
}

// Test Topic Isolation (Publish to A does not reach B)
TEST(PubSubTest, MultipleTopics) {
    PubSub ps;
    auto subA = ps.Subscribe<int>("topic_A");
    auto subB = ps.Subscribe<int>("topic_B");
    
    ps.Publish<int>("topic_A", 100);
    ps.Publish<int>("topic_B", 200);
    
    auto msgA = subA->try_receive();
    auto msgB = subB->try_receive();
    
    ASSERT_TRUE(msgA.has_value());
    ASSERT_TRUE(msgB.has_value());
    EXPECT_EQ(msgA.value(), 100);
    EXPECT_EQ(msgB.value(), 200);
}

// Test that dropped subscribers are cleaned up safely
TEST(PubSubTest, SubscriberAutoCleanup) {
    PubSub ps;
    {
        auto sub = ps.Subscribe<int>("cleanup_topic");
        ps.Publish<int>("cleanup_topic", 1);
        auto msg = sub->try_receive();
        EXPECT_TRUE(msg.has_value());
    } // sub goes out of scope here

    // Publishing should not crash, it will clean up the dead weak_ptr
    EXPECT_NO_THROW({
        ps.Publish<int>("cleanup_topic", 2);
    });
}

// Test Thread Safety with concurrent publishers
TEST(PubSubTest, ThreadSafety) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("thread_topic", 10000); // large capacity
    
    const int NUM_THREADS = 10;
    const int MESSAGES_PER_THREAD = 100;
    
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&ps, MESSAGES_PER_THREAD]() {
            for (int j = 0; j < MESSAGES_PER_THREAD; ++j) {
                ps.Publish<int>("thread_topic", 1);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Now count the received messages
    int count = 0;
    while (auto msg = sub->try_receive()) {
        count += msg.value();
    }
    
    EXPECT_EQ(count, NUM_THREADS * MESSAGES_PER_THREAD);
}

// Test Selector functionality
TEST(SelectorTest, BasicSelection) {
    PubSub ps;
    auto sub1 = ps.Subscribe<int>("sel1");
    auto sub2 = ps.Subscribe<int>("sel2");
    
    Selector selector;
    int sum = 0;
    
    selector.Add<int>(sub1, [&sum](const int& val) { sum += val; });
    selector.Add<int>(sub2, [&sum](const int& val) { sum += val; });
    
    ps.Publish<int>("sel1", 10);
    ps.Publish<int>("sel2", 20);
    
    // Wait for at most 100ms for events to be processed
    bool processed = selector.WaitFor(std::chrono::milliseconds(100));
    
    EXPECT_TRUE(processed);
    EXPECT_EQ(sum, 30);
}

// Test OverflowPolicy::DropOldest
TEST(PubSubTest, OverflowPolicyDropOldest) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("drop_oldest_topic", 2, OverflowPolicy::DropOldest);
    
    ps.Publish<int>("drop_oldest_topic", 1);
    ps.Publish<int>("drop_oldest_topic", 2);
    ps.Publish<int>("drop_oldest_topic", 3); // 1 should be dropped
    
    auto msg1 = sub->try_receive();
    auto msg2 = sub->try_receive();
    auto msg3 = sub->try_receive();
    
    ASSERT_TRUE(msg1.has_value());
    EXPECT_EQ(msg1.value(), 2);
    ASSERT_TRUE(msg2.has_value());
    EXPECT_EQ(msg2.value(), 3);
    EXPECT_FALSE(msg3.has_value());
}

// Test OverflowPolicy::DropNewest
TEST(PubSubTest, OverflowPolicyDropNewest) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("drop_newest_topic", 2, OverflowPolicy::DropNewest);
    
    ps.Publish<int>("drop_newest_topic", 1);
    ps.Publish<int>("drop_newest_topic", 2);
    ps.Publish<int>("drop_newest_topic", 3); // 3 should be dropped
    
    auto msg1 = sub->try_receive();
    auto msg2 = sub->try_receive();
    auto msg3 = sub->try_receive();
    
    ASSERT_TRUE(msg1.has_value());
    EXPECT_EQ(msg1.value(), 1);
    ASSERT_TRUE(msg2.has_value());
    EXPECT_EQ(msg2.value(), 2);
    EXPECT_FALSE(msg3.has_value());
}

// Test Type Mismatch Exception
TEST(PubSubTest, TypeMismatchException) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("type_topic");
    
    EXPECT_THROW({
        ps.Publish<double>("type_topic", 3.14);
    }, std::invalid_argument);
}

// Test Subscriber Close
TEST(PubSubTest, SubscriberClose) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("close_topic");
    
    ps.Publish<int>("close_topic", 1);
    sub->close();
    ps.Publish<int>("close_topic", 2); // Should not be pushed
    
    auto msg1 = sub->try_receive();
    auto msg2 = sub->try_receive();
    
    ASSERT_TRUE(msg1.has_value());
    EXPECT_EQ(msg1.value(), 1);
    EXPECT_FALSE(msg2.has_value());
}

// Test Selector Timeout
TEST(SelectorTest, Timeout) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("timeout_topic");
    
    Selector selector;
    bool called = false;
    selector.Add<int>(sub, [&called](const int&) { called = true; });
    
    bool processed = selector.WaitFor(std::chrono::milliseconds(50));
    EXPECT_FALSE(processed);
    EXPECT_FALSE(called);
}

// Test Selector with Multiple Types
TEST(SelectorTest, MultipleTypes) {
    PubSub ps;
    auto subInt = ps.Subscribe<int>("int_sel");
    auto subStr = ps.Subscribe<std::string>("str_sel");
    
    Selector selector;
    int int_val = 0;
    std::string str_val = "";
    
    selector.Add<int>(subInt, [&int_val](const int& val) { int_val = val; });
    selector.Add<std::string>(subStr, [&str_val](const std::string& val) { str_val = val; });
    
    ps.Publish<int>("int_sel", 42);
    selector.WaitFor(std::chrono::milliseconds(100));
    EXPECT_EQ(int_val, 42);
    
    ps.Publish<std::string>("str_sel", std::string("hello"));
    selector.WaitFor(std::chrono::milliseconds(100));
    EXPECT_EQ(str_val, "hello");
}

// Test Worker
TEST(WorkerTest, BasicWorker) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("worker_topic");
    
    Worker worker;
    std::atomic<int> sum{0};
    std::atomic<int> ticks{0};
    
    worker.AddSubscription<int>(sub, [&sum](const int& val) { sum += val; });
    worker.SetTickCallback(std::chrono::milliseconds(10), [&ticks]() { ticks++; });
    
    worker.Start();
    
    ps.Publish<int>("worker_topic", 10);
    ps.Publish<int>("worker_topic", 20);
    
    // Give the background worker some time to process
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    worker.Stop();
    
    EXPECT_EQ(sum.load(), 30);
    EXPECT_GT(ticks.load(), 0);
}

// Test Shared Pointer Cleanup
TEST(PubSubTest, SharedPtrMessageCleanup) {
    PubSub ps;
    auto sub = ps.Subscribe<std::shared_ptr<int>>("shared_ptr_topic", 2, OverflowPolicy::DropOldest);
    
    std::weak_ptr<int> weak1, weak2, weak3;
    
    {
        auto ptr1 = std::make_shared<int>(1);
        auto ptr2 = std::make_shared<int>(2);
        auto ptr3 = std::make_shared<int>(3);
        
        weak1 = ptr1;
        weak2 = ptr2;
        weak3 = ptr3;
        
        ps.Publish<std::shared_ptr<int>>("shared_ptr_topic", ptr1);
        ps.Publish<std::shared_ptr<int>>("shared_ptr_topic", ptr2);
        ps.Publish<std::shared_ptr<int>>("shared_ptr_topic", ptr3); // ptr1 should be dropped
    }
    
    // ptr1 was dropped due to DropOldest, so its reference count should be 0
    EXPECT_TRUE(weak1.expired());
    // ptr2 and ptr3 are still in the queue
    EXPECT_FALSE(weak2.expired());
    EXPECT_FALSE(weak3.expired());
    
    {
        auto msg = sub->try_receive();
        ASSERT_TRUE(msg.has_value());
        EXPECT_EQ(*msg.value(), 2);
    } // msg goes out of scope, ptr2 is destroyed
    
    EXPECT_TRUE(weak2.expired());
    EXPECT_FALSE(weak3.expired()); // ptr3 is still in the queue
    
    // Destroying the subscriber should clear its queue and destroy ptr3
    sub.reset();
    
    EXPECT_TRUE(weak3.expired());
}
