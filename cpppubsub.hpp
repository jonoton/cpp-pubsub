#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <chrono>
#include <functional>
#include <optional>
#include <typeindex>
#include <stdexcept>
#include <iostream>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>
#endif

namespace cpppubsub
{

    constexpr int VERSION_MAJOR = 1;
    constexpr int VERSION_MINOR = 1;
    constexpr int VERSION_PATCH = 1;

    /**
     * @brief Returns the library version as a string.
     * @return The version string in "MAJOR.MINOR.PATCH" format.
     */
    inline std::string version()
    {
        return std::to_string(VERSION_MAJOR) + "." +
               std::to_string(VERSION_MINOR) + "." +
               std::to_string(VERSION_PATCH);
    }

    /**
     * @brief Defines the policy for handling a full subscriber queue.
     */
    enum class OverflowPolicy
    {
        Block,      ///< Put publisher to sleep until space frees up.
        DropOldest, ///< Discard the oldest message to make room.
        DropNewest  ///< Discard the incoming message.
    };

    /**
     * @brief A thread-safe queue that receives messages from a topic.
     * @tparam T The message type.
     */
    template <typename T>
    class Subscriber
    {
    private:
        std::deque<T> queue_;
        std::mutex mtx_;
        bool closed_ = false;

        size_t max_capacity_;
        OverflowPolicy overflow_policy_;
        std::condition_variable cv_space_;

#ifdef _WIN32
        HANDLE event_;
#else
        int pipe_fds_[2]; // 0 is read, 1 is write
#endif

    public:
        /**
         * @brief Constructs a new Subscriber.
         * @param max_capacity The maximum number of messages the queue can hold.
         * @param policy The policy to apply when the queue is full.
         */
        Subscriber(size_t max_capacity = 1000, OverflowPolicy policy = OverflowPolicy::Block)
            : max_capacity_(max_capacity), overflow_policy_(policy)
        {
#ifdef _WIN32
            // Auto-reset event
            event_ = CreateEvent(NULL, FALSE, FALSE, NULL);
            if (!event_)
                throw std::runtime_error("Failed to create Windows Event");
#else
            if (pipe(pipe_fds_) != 0)
                throw std::runtime_error("Failed to create POSIX pipe");
            // Set both ends to non-blocking
            fcntl(pipe_fds_[0], F_SETFL, O_NONBLOCK);
            fcntl(pipe_fds_[1], F_SETFL, O_NONBLOCK);
#endif
        }

        ~Subscriber()
        {
            close();
#ifdef _WIN32
            if (event_)
                CloseHandle(event_);
#else
            ::close(pipe_fds_[0]);
            ::close(pipe_fds_[1]);
#endif
        }

#ifdef _WIN32
        /**
         * @brief Retrieves the Windows event handle for polling.
         * @return The HANDLE object.
         */
        HANDLE GetWaitHandle() const { return event_; }
#else
        /**
         * @brief Retrieves the POSIX file descriptor for polling.
         * @return The file descriptor.
         */
        int GetWaitFD() const { return pipe_fds_[0]; }
#endif

        /**
         * @brief Pushes a message into the subscriber's queue.
         * @param message The message to push.
         */
        void push(const T &message)
        {
            {
                std::unique_lock<std::mutex> lock(mtx_);

                if (closed_)
                    return;

                if (queue_.size() >= max_capacity_)
                {
                    if (overflow_policy_ == OverflowPolicy::Block)
                    {
                        cv_space_.wait(lock, [this]()
                                       { return queue_.size() < max_capacity_ || closed_; });
                        if (closed_)
                            return;
                    }
                    else if (overflow_policy_ == OverflowPolicy::DropOldest)
                    {
                        queue_.pop_front();
                    }
                    else if (overflow_policy_ == OverflowPolicy::DropNewest)
                    {
                        return;
                    }
                }
                queue_.push_back(message);
            }

            // Signal OS event (outside of mutex lock)
#ifdef _WIN32
            SetEvent(event_);
#else
            char c = 1;
            [[maybe_unused]] auto res = write(pipe_fds_[1], &c, 1);
#endif
        }

        /**
         * @brief Attempts to receive a message from the queue without blocking.
         * @return An optional containing the message if the queue was not empty, otherwise std::nullopt.
         */
        std::optional<T> try_receive()
        {
            bool popped = false;
            T msg;

            {
                std::lock_guard<std::mutex> lock(mtx_);
                if (queue_.empty())
                {
#ifndef _WIN32
                    // Clear the pipe so poll stops triggering
                    char c;
                    while (read(pipe_fds_[0], &c, 1) > 0)
                    {
                    }
#endif
                    return std::nullopt;
                }

                msg = std::move(queue_.front());
                queue_.pop_front();
                popped = true;

#ifdef _WIN32
                if (!queue_.empty())
                {
                    SetEvent(event_);
                }
#else
                if (queue_.empty())
                {
                    // Clear the pipe
                    char c;
                    while (read(pipe_fds_[0], &c, 1) > 0)
                    {
                    }
                }
                else
                {
                    // Ensure poll will trigger for remaining items
                    char c = 1;
                    [[maybe_unused]] auto res = write(pipe_fds_[1], &c, 1);
                }
#endif
            }

            // Notify sleeping publishers (if any) that space is available
            if (popped)
                cv_space_.notify_one();

            return msg;
        }

        /**
         * @brief Closes the subscriber, preventing new messages from being pushed
         * and waking up any waiting publishers or selectors.
         */
        void close()
        {
            {
                std::lock_guard<std::mutex> lock(mtx_);
                if (closed_)
                    return;
                closed_ = true;
            }
#ifdef _WIN32
            SetEvent(event_);
#else
            char c = 1;
            [[maybe_unused]] auto res = write(pipe_fds_[1], &c, 1);
#endif
            cv_space_.notify_all();
        }
    };

    /**
     * @brief Base class for type-erased topics.
     */
    class TopicBase
    {
    public:
        virtual ~TopicBase() = default;
    };

    /**
     * @brief A topic that manages subscribers and broadcasts messages.
     * @tparam T The message type.
     */
    template <typename T>
    class Topic : public TopicBase
    {
    private:
        std::mutex mtx_;
        std::vector<std::weak_ptr<Subscriber<T>>> subscribers_;

    public:
        /**
         * @brief Adds a subscriber to this topic.
         * @param sub A shared pointer to the subscriber.
         */
        void AddSubscriber(std::shared_ptr<Subscriber<T>> sub)
        {
            std::lock_guard<std::mutex> lock(mtx_);
            subscribers_.push_back(sub);
        }

        /**
         * @brief Publishes a message to all active subscribers.
         * @param message The message to publish.
         */
        void Publish(const T &message)
        {
            std::vector<std::shared_ptr<Subscriber<T>>> active_subs;
            {
                std::lock_guard<std::mutex> lock(mtx_);
                for (auto it = subscribers_.begin(); it != subscribers_.end();)
                {
                    if (auto sub = it->lock())
                    {
                        active_subs.push_back(sub);
                        ++it;
                    }
                    else
                    {
                        it = subscribers_.erase(it); // Auto-cleanup dead subscribers
                    }
                }
            }

            for (auto &sub : active_subs)
            {
                sub->push(message);
            }
        }
    };

    /**
     * @brief A publisher object that allows bypassing the broker map lookup for high performance.
     * @tparam T The message type.
     */
    template <typename T>
    class Publisher
    {
    private:
        std::shared_ptr<Topic<T>> topic_;

    public:
        explicit Publisher(std::shared_ptr<Topic<T>> topic) : topic_(std::move(topic)) {}

        /**
         * @brief Publishes a message directly to the topic.
         * @param message The message to publish.
         */
        void Publish(const T &message)
        {
            topic_->Publish(message);
        }
    };

    /**
     * @brief Manager class that coordinates topics and subscriptions.
     */
    class PubSub
    {
    private:
        std::mutex map_mutex_;
        std::unordered_map<std::string, std::shared_ptr<TopicBase>> topics_;

        template <typename T>
        std::shared_ptr<Topic<T>> GetOrCreateTopic(const std::string &name)
        {
            std::lock_guard<std::mutex> lock(map_mutex_);
            auto it = topics_.find(name);
            if (it != topics_.end())
            {
                auto topic = std::dynamic_pointer_cast<Topic<T>>(it->second);
                if (!topic)
                    throw std::invalid_argument("Type mismatch on topic: " + name);
                return topic;
            }

            auto new_topic = std::make_shared<Topic<T>>();
            topics_[name] = new_topic;
            return new_topic;
        }

    public:
        /**
         * @brief Subscribes to a topic.
         * @tparam T The message type.
         * @param name The name of the topic.
         * @param capacity The maximum capacity of the subscriber queue.
         * @param policy The overflow policy.
         * @return A shared pointer to the new subscriber.
         */
        template <typename T>
        std::shared_ptr<Subscriber<T>> Subscribe(const std::string &name, size_t capacity = 1000, OverflowPolicy policy = OverflowPolicy::Block)
        {
            auto topic = GetOrCreateTopic<T>(name);
            auto sub = std::make_shared<Subscriber<T>>(capacity, policy);
            topic->AddSubscriber(sub);
            return sub;
        }

        /**
         * @brief Creates a high-performance publisher for a topic.
         * @tparam T The message type.
         * @param name The name of the topic.
         * @return A Publisher object for the topic.
         */
        template <typename T>
        Publisher<T> CreatePublisher(const std::string &name)
        {
            auto topic = GetOrCreateTopic<T>(name);
            return Publisher<T>(topic);
        }

        /**
         * @brief Publishes a message to a topic.
         * @tparam T The message type.
         * @param name The name of the topic.
         * @param message The message to publish.
         */
        template <typename T>
        void Publish(const std::string &name, const T &message)
        {
            auto topic = GetOrCreateTopic<T>(name);
            topic->Publish(message);
        }
    };

    /**
     * @brief Multiplexer for polling multiple subscribers efficiently.
     */
    class Selector
    {
    private:
        struct WaitTarget
        {
#ifdef _WIN32
            HANDLE handle;
#else
            int fd;
#endif
            std::function<void()> process_all;
        };

        std::vector<WaitTarget> targets_;

    public:
        /**
         * @brief Adds a subscriber to the selector with a callback.
         * @tparam T The message type.
         * @param sub The subscriber to monitor.
         * @param callback The function to call when a message is received.
         */
        template <typename T>
        void Add(std::shared_ptr<Subscriber<T>> sub, std::function<void(const T &)> callback)
        {
            WaitTarget target;
#ifdef _WIN32
            target.handle = sub->GetWaitHandle();
#else
            target.fd = sub->GetWaitFD();
#endif
            target.process_all = [sub, callback]()
            {
                while (auto msg = sub->try_receive())
                {
                    callback(*msg);
                }
            };
            targets_.push_back(std::move(target));
        }

        /**
         * @brief Waits for messages on all monitored subscribers and processes them.
         * @tparam Rep The representation type for duration.
         * @tparam Period The period type for duration.
         * @param timeout The maximum duration to wait.
         * @return True if any events were processed, false if a timeout occurred.
         */
        template <typename Rep, typename Period>
        bool WaitFor(const std::chrono::duration<Rep, Period> &timeout)
        {
            if (targets_.empty())
                return false;
            auto timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count();

#ifdef _WIN32
            std::vector<HANDLE> handles;
            for (const auto &t : targets_)
                handles.push_back(t.handle);

            DWORD result = WaitForMultipleObjects(
                static_cast<DWORD>(handles.size()), handles.data(), FALSE, static_cast<DWORD>(timeout_ms));

            if (result >= WAIT_OBJECT_0 && result < WAIT_OBJECT_0 + handles.size())
            {
                targets_[result - WAIT_OBJECT_0].process_all();
                return true;
            }
            return false;
#else
            std::vector<struct pollfd> fds;
            for (const auto &t : targets_)
                fds.push_back({t.fd, POLLIN, 0});

            int result = poll(fds.data(), fds.size(), static_cast<int>(timeout_ms));
            if (result > 0)
            {
                for (size_t i = 0; i < fds.size(); ++i)
                {
                    if (fds[i].revents & POLLIN)
                        targets_[i].process_all();
                }
                return true;
            }
            return false;
#endif
        }
    };

    /**
     * @brief A background worker that automatically polls a selector.
     */
    class Worker
    {
    private:
        std::atomic<bool> running_{false};
        std::thread thread_;
        Selector selector_;
        std::function<void()> tick_callback_;
        std::chrono::milliseconds timeout_{50};

        void ThreadLoop()
        {
            while (running_.load(std::memory_order_relaxed))
            {
                selector_.WaitFor(timeout_);
                if (tick_callback_)
                    tick_callback_();
            }
        }

    public:
        Worker() = default;
        Worker(const Worker &) = delete;
        Worker &operator=(const Worker &) = delete;

        ~Worker() { Stop(); }

        /**
         * @brief Adds a subscription to the background worker.
         * @tparam T The message type.
         * @param sub The subscriber to monitor.
         * @param callback The function to call when a message is received.
         */
        template <typename T>
        inline void AddSubscription(std::shared_ptr<Subscriber<T>> sub, std::function<void(const T &)> callback)
        {
            selector_.Add(sub, std::move(callback));
        }

        /**
         * @brief Sets a callback to be invoked periodically (on every tick).
         * @param timeout The maximum duration to wait between ticks.
         * @param callback The function to call on tick.
         */
        inline void SetTickCallback(std::chrono::milliseconds timeout, std::function<void()> callback)
        {
            timeout_ = timeout;
            tick_callback_ = std::move(callback);
        }

        /**
         * @brief Starts the background worker thread.
         */
        inline void Start()
        {
            bool expected = false;
            if (running_.compare_exchange_strong(expected, true))
            {
                thread_ = std::thread(&Worker::ThreadLoop, this);
            }
        }

        /**
         * @brief Stops the background worker thread and blocks until it finishes.
         */
        inline void Stop()
        {
            if (running_.exchange(false))
            {
                if (thread_.joinable())
                    thread_.join();
            }
        }
    };
}
