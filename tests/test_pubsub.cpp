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

// Test Topic Mutex Block Issue
TEST(PubSubTest, TopicPublishDoesNotBlockOnFullQueue) {
    PubSub ps;
    // Create a subscriber with capacity 1 and Block policy
    auto sub1 = ps.Subscribe<int>("block_topic", 1, OverflowPolicy::Block);
    
    // Fill the queue
    ps.Publish<int>("block_topic", 1);
    
    // Spawn a thread to publish a second message.
    // This will block because the queue is full.
    std::atomic<bool> thread_blocked{true};
    std::thread t([&]() {
        ps.Publish<int>("block_topic", 2);
        thread_blocked = false;
    });
    
    // Give the thread a moment to block inside Publish
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // If the bug is present, the Topic mutex is held by the background thread,
    // so AddSubscriber (or Subscribe) will hang indefinitely.
    // We expect this to complete almost instantly now.
    auto start = std::chrono::steady_clock::now();
    auto sub2 = ps.Subscribe<int>("block_topic", 10, OverflowPolicy::Block);
    auto end = std::chrono::steady_clock::now();
    
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    EXPECT_LT(duration_ms, 50); // Should be very fast, well under 50ms
    
    // Now pop the first message, which should unblock the thread
    auto msg1 = sub1->try_receive();
    EXPECT_TRUE(msg1.has_value());
    EXPECT_EQ(msg1.value(), 1);
    
    t.join();
    EXPECT_FALSE(thread_blocked.load());
    
    auto msg2 = sub1->try_receive();
    EXPECT_TRUE(msg2.has_value());
    EXPECT_EQ(msg2.value(), 2);
}

// Test Publisher class
TEST(PubSubTest, PublisherClass) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("pub_class_topic");
    
    auto publisher = ps.CreatePublisher<int>("pub_class_topic");
    publisher.Publish(42);
    publisher.Publish(100);
    
    auto msg1 = sub->try_receive();
    auto msg2 = sub->try_receive();
    auto msg3 = sub->try_receive();
    
    ASSERT_TRUE(msg1.has_value());
    EXPECT_EQ(msg1.value(), 42);
    ASSERT_TRUE(msg2.has_value());
    EXPECT_EQ(msg2.value(), 100);
    EXPECT_FALSE(msg3.has_value());
}

// Test Publisher Thread Safety
TEST(PubSubTest, PublisherThreadSafety) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("pub_thread_topic", 10000);
    auto publisher = ps.CreatePublisher<int>("pub_thread_topic");
    
    const int NUM_THREADS = 10;
    const int MESSAGES_PER_THREAD = 100;
    
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
        // Pass publisher by value to test copy semantics implicitly
        threads.emplace_back([publisher, MESSAGES_PER_THREAD]() mutable {
            for (int j = 0; j < MESSAGES_PER_THREAD; ++j) {
                publisher.Publish(1);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    int count = 0;
    while (auto msg = sub->try_receive()) {
        count += msg.value();
    }
    
    EXPECT_EQ(count, NUM_THREADS * MESSAGES_PER_THREAD);
}

// Test Selector behavior when a registered subscriber is destroyed
TEST(SelectorTest, SubscriberDestructionNoBusyLoop) {
    PubSub ps;
    Selector selector;
    std::atomic<int> sum{0};

    // Keep one subscriber alive so the selector has at least one active channel to wait on
    auto sub_alive = ps.Subscribe<int>("alive_topic");
    selector.Add<int>(sub_alive, [](const int&) {});

    {
        auto sub = ps.Subscribe<int>("temp_topic");
        selector.Add<int>(sub, [&sum](const int& val) { sum += val; });

        ps.Publish<int>("temp_topic", 10);
        // Wait and process
        bool processed = selector.WaitFor(std::chrono::milliseconds(50));
        EXPECT_TRUE(processed);
        EXPECT_EQ(sum.load(), 10);
    } // sub is destroyed here, closing its event/pipe

    // Call WaitFor. If the busy loop bug exists, this will return immediately due to POLLNVAL/invalid handle.
    // When fixed, it should clean up the expired subscriber and block for the full timeout on the alive subscriber.
    auto start = std::chrono::steady_clock::now();
    bool processed = selector.WaitFor(std::chrono::milliseconds(50));
    auto end = std::chrono::steady_clock::now();

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    EXPECT_FALSE(processed);
    // It should have waited for the full timeout since no events are pending on the alive subscriber
    EXPECT_GE(duration_ms, 40); 
}

// Test Zero Capacity throws std::invalid_argument
TEST(PubSubTest, ZeroCapacityException) {
    PubSub ps;
    EXPECT_THROW({
        ps.Subscribe<int>("zero_capacity", 0);
    }, std::invalid_argument);
}

// Test Explicit Unsubscribe
TEST(PubSubTest, Unsubscribe) {
    PubSub ps;
    auto sub1 = ps.Subscribe<int>("unsub_topic");
    auto sub2 = ps.Subscribe<int>("unsub_topic");

    ps.Publish<int>("unsub_topic", 10);
    
    // Both should receive the message
    auto msg1 = sub1->try_receive();
    auto msg2 = sub2->try_receive();
    ASSERT_TRUE(msg1.has_value());
    ASSERT_TRUE(msg2.has_value());
    EXPECT_EQ(msg1.value(), 10);
    EXPECT_EQ(msg2.value(), 10);

    // Unsubscribe sub1
    ps.Unsubscribe<int>("unsub_topic", sub1);

    ps.Publish<int>("unsub_topic", 20);

    // sub1 should not get it, sub2 should
    auto msg1_after = sub1->try_receive();
    auto msg2_after = sub2->try_receive();
    EXPECT_FALSE(msg1_after.has_value());
    ASSERT_TRUE(msg2_after.has_value());
    EXPECT_EQ(msg2_after.value(), 20);
}

// Test explicit topic removal and clear
TEST(PubSubTest, TopicLifecycle) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("lifecycle_topic");
    ps.Publish<int>("lifecycle_topic", 100);

    // Remove topic
    ps.RemoveTopic("lifecycle_topic");

    // Publishing to "lifecycle_topic" now will create a NEW topic, so "sub" won't get it
    ps.Publish<int>("lifecycle_topic", 200);

    auto msg1 = sub->try_receive();
    ASSERT_TRUE(msg1.has_value());
    EXPECT_EQ(msg1.value(), 100); // from the old topic

    auto msg2 = sub->try_receive();
    EXPECT_FALSE(msg2.has_value()); // didn't get 200 because topic was recreated
}

// Test Move-Only type support
TEST(PubSubTest, MoveOnlyType) {
    PubSub ps;
    auto sub = ps.Subscribe<std::unique_ptr<int>>("move_only_topic");

    auto ptr = std::make_unique<int>(1337);
    ps.Publish("move_only_topic", std::move(ptr));

    auto msg = sub->try_receive();
    ASSERT_TRUE(msg.has_value());
    ASSERT_NE(msg.value(), nullptr);
    EXPECT_EQ(*msg.value(), 1337);
}

// Test that publishing a move-only type with multiple active subscribers throws an exception
TEST(PubSubTest, MoveOnlyTypeMultipleSubscribersThrows) {
    PubSub ps;
    auto sub1 = ps.Subscribe<std::unique_ptr<int>>("move_only_multi");
    auto sub2 = ps.Subscribe<std::unique_ptr<int>>("move_only_multi");

    auto ptr = std::make_unique<int>(42);
    EXPECT_THROW({
        ps.Publish("move_only_multi", std::move(ptr));
    }, std::runtime_error);
}

// Custom move-only type that implements a clone() method
struct CustomMoveOnly {
    int val;
    explicit CustomMoveOnly(int v) : val(v) {}
    CustomMoveOnly(const CustomMoveOnly&) = delete;
    CustomMoveOnly& operator=(const CustomMoveOnly&) = delete;
    CustomMoveOnly(CustomMoveOnly&&) = default;
    CustomMoveOnly& operator=(CustomMoveOnly&&) = default;

    CustomMoveOnly clone() const {
        return CustomMoveOnly(val);
    }
};

// Test that move-only types with a clone() method can be published to multiple subscribers successfully
TEST(PubSubTest, MoveOnlyCloneableType) {
    PubSub ps;
    auto sub1 = ps.Subscribe<CustomMoveOnly>("cloneable_multi");
    auto sub2 = ps.Subscribe<CustomMoveOnly>("cloneable_multi");

    CustomMoveOnly msg(99);
    ps.Publish("cloneable_multi", std::move(msg));

    auto r1 = sub1->try_receive();
    auto r2 = sub2->try_receive();

    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r1->val, 99);
    EXPECT_EQ(r2->val, 99);
}

// Another move-only type without clone() that uses template specialization of Cloner
struct SpecializedMoveOnly {
    int val;
    explicit SpecializedMoveOnly(int v) : val(v) {}
    SpecializedMoveOnly(const SpecializedMoveOnly&) = delete;
    SpecializedMoveOnly& operator=(const SpecializedMoveOnly&) = delete;
    SpecializedMoveOnly(SpecializedMoveOnly&&) = default;
    SpecializedMoveOnly& operator=(SpecializedMoveOnly&&) = default;
};

namespace cpppubsub {
    template <>
    struct Cloner<SpecializedMoveOnly> {
        static SpecializedMoveOnly perform(const SpecializedMoveOnly& val) {
            return SpecializedMoveOnly{val.val};
        }
    };
}

// Test that move-only types with specialized Cloner can be published to multiple subscribers successfully
TEST(PubSubTest, MoveOnlySpecializedClonerType) {
    PubSub ps;
    auto sub1 = ps.Subscribe<SpecializedMoveOnly>("specialized_multi");
    auto sub2 = ps.Subscribe<SpecializedMoveOnly>("specialized_multi");

    SpecializedMoveOnly msg(55);
    ps.Publish("specialized_multi", std::move(msg));

    auto r1 = sub1->try_receive();
    auto r2 = sub2->try_receive();

    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r1->val, 55);
    EXPECT_EQ(r2->val, 55);
}

// Test that Selector supports move-only type callbacks and successfully transfers ownership
TEST(SelectorTest, MoveOnlyTypeCallback) {
    PubSub ps;
    auto sub = ps.Subscribe<std::unique_ptr<int>>("move_only_sel");

    Selector selector;
    std::unique_ptr<int> received_ptr;
    
    // Callback takes std::unique_ptr<int> by value to take ownership!
    selector.Add<std::unique_ptr<int>>(sub, [&received_ptr](std::unique_ptr<int> ptr) {
        received_ptr = std::move(ptr);
    });

    auto ptr = std::make_unique<int>(999);
    ps.Publish("move_only_sel", std::move(ptr));

    bool processed = selector.WaitFor(std::chrono::milliseconds(100));
    EXPECT_TRUE(processed);
    ASSERT_NE(received_ptr, nullptr);
    EXPECT_EQ(*received_ptr, 999);
}

// Test that Selector correctly handles extremely large timeout values without overflow/indefinite wait
TEST(SelectorTest, LargeTimeoutClamping) {
    PubSub ps;
    auto sub = ps.Subscribe<int>("large_timeout_topic");

    Selector selector;
    std::atomic<int> received_val{0};
    selector.Add<int>(sub, [&received_val](const int& val) {
        received_val = val;
    });

    // Start a thread to publish a message after 20ms
    std::thread t([&ps]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        ps.Publish<int>("large_timeout_topic", 123);
    });

    // Call WaitFor with an extremely large timeout (1 year)
    // If integer overflow occurs, this might wait indefinitely or crash.
    // Clamping to a max safe value ensures it successfully waits and wakes up when the thread publishes.
    bool processed = selector.WaitFor(std::chrono::hours(24 * 365));
    t.join();

    EXPECT_TRUE(processed);
    EXPECT_EQ(received_val.load(), 123);
}




