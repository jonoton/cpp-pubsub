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
#include <algorithm>
#include <type_traits>
#include <limits>

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
    constexpr int VERSION_MINOR = 4;
    constexpr int VERSION_PATCH = 0;

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

#ifdef _WIN32
    namespace detail
    {
        inline VOID CALLBACK SelectorWaitCallback(PVOID lpParam, BOOLEAN TimerOrWaitFired)
        {
            if (!TimerOrWaitFired)
            {
                SetEvent(static_cast<HANDLE>(lpParam));
            }
        }
    }
#endif

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
            if (max_capacity == 0)
                throw std::invalid_argument("max_capacity must be greater than 0");

#ifdef _WIN32
            // Manual-reset event
            event_ = CreateEvent(NULL, TRUE, FALSE, NULL);
            if (!event_)
                throw std::runtime_error("Failed to create Windows Event");
#else
            if (pipe(pipe_fds_) != 0)
                throw std::runtime_error("Failed to create POSIX pipe");
            // Set both ends to non-blocking
            if (fcntl(pipe_fds_[0], F_SETFL, O_NONBLOCK) == -1)
            {
                ::close(pipe_fds_[0]);
                ::close(pipe_fds_[1]);
                throw std::runtime_error("Failed to set pipe read end to non-blocking");
            }
            if (fcntl(pipe_fds_[1], F_SETFL, O_NONBLOCK) == -1)
            {
                ::close(pipe_fds_[0]);
                ::close(pipe_fds_[1]);
                throw std::runtime_error("Failed to set pipe write end to non-blocking");
            }
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

        // Subscriber is non-copyable and non-movable to ensure resource-safety
        Subscriber(const Subscriber &) = delete;
        Subscriber &operator=(const Subscriber &) = delete;
        Subscriber(Subscriber &&) = delete;
        Subscriber &operator=(Subscriber &&) = delete;

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
            static_assert(std::is_copy_constructible_v<T>,
                          "push(const T&) requires copy-constructible type T. Use push(T&&) for move-only types.");

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
            bool needs_signal = queue_.empty();
            queue_.push_back(message);

            // Signal OS event (inside mutex lock to prevent lost wakeups)
            if (needs_signal)
            {
#ifdef _WIN32
                SetEvent(event_);
#else
                char c = 1;
                [[maybe_unused]] auto res = write(pipe_fds_[1], &c, 1);
#endif
            }
        }

        /**
         * @brief Pushes a message into the subscriber's queue by moving.
         * @param message The message to push.
         */
        void push(T &&message)
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
            bool needs_signal = queue_.empty();
            queue_.push_back(std::move(message));

            // Signal OS event (inside mutex lock to prevent lost wakeups)
            if (needs_signal)
            {
#ifdef _WIN32
                SetEvent(event_);
#else
                char c = 1;
                [[maybe_unused]] auto res = write(pipe_fds_[1], &c, 1);
#endif
            }
        }

        /**
         * @brief Attempts to receive a message from the queue without blocking.
         * @return An optional containing the message if the queue was not empty, otherwise std::nullopt.
         */
        std::optional<T> try_receive()
        {
            bool popped = false;
            std::optional<T> msg;

            {
                std::lock_guard<std::mutex> lock(mtx_);
                if (queue_.empty())
                {
#ifdef _WIN32
                    ResetEvent(event_);
#else
                    // Clear the pipe so poll stops triggering (inside lock to prevent races)
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

                if (queue_.empty())
                {
#ifdef _WIN32
                    ResetEvent(event_);
#else
                    // Clear the pipe (inside lock to prevent races)
                    char c;
                    while (read(pipe_fds_[0], &c, 1) > 0)
                    {
                    }
#endif
                }
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
            std::lock_guard<std::mutex> lock(mtx_);
            if (closed_)
                return;
            closed_ = true;
#ifdef _WIN32
            SetEvent(event_);
#else
            char c = 1;
            [[maybe_unused]] auto res = write(pipe_fds_[1], &c, 1);
#endif
            cv_space_.notify_all();
        }
    };

    // Type trait to detect if T has a .clone() const member function
    template <typename T, typename = void>
    struct has_clone : std::false_type
    {
    };

    template <typename T>
    struct has_clone<T, std::void_t<decltype(std::declval<const T &>().clone())>> : std::true_type
    {
    };

    // Extensible Cloner template that can be specialized by users for custom types
    template <typename T, typename = void>
    struct Cloner
    {
        template <typename U = T>
        static auto perform(const U &val) -> typename std::enable_if_t<std::is_copy_constructible_v<U> || has_clone<U>::value, U>
        {
            if constexpr (std::is_copy_constructible_v<U>)
            {
                return val;
            }
            else
            {
                return val.clone();
            }
        }
    };

    // Trait to check if a type is cloneable/copyable via Cloner
    template <typename T, typename = void>
    struct is_cloneable : std::false_type
    {
    };

    template <typename T>
    struct is_cloneable<T, std::void_t<decltype(Cloner<T>::perform(std::declval<const T &>()))>> : std::true_type
    {
    };

    template <typename T>
    constexpr bool is_cloneable_v = is_cloneable<T>::value;

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
         * @brief Explicitly removes a subscriber from this topic.
         * @param sub A shared pointer to the subscriber to remove.
         */
        void RemoveSubscriber(const std::shared_ptr<Subscriber<T>> &sub)
        {
            std::lock_guard<std::mutex> lock(mtx_);
            subscribers_.erase(
                std::remove_if(subscribers_.begin(), subscribers_.end(),
                               [&sub](const std::weak_ptr<Subscriber<T>> &weak_sub)
                               {
                                   auto shared = weak_sub.lock();
                                   return !shared || shared == sub;
                               }),
                subscribers_.end());
        }

        /**
         * @brief Publishes a message to all active subscribers.
         * @param message The message to publish.
         */
        void Publish(const T &message)
        {
            if constexpr (is_cloneable_v<T>)
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
                    sub->push(Cloner<T>::perform(message));
                }
            }
        }

        /**
         * @brief Publishes a message to all active subscribers by moving.
         * @param message The message to publish.
         */
        void Publish(T &&message)
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

            if (active_subs.empty())
                return;

            if (active_subs.size() == 1)
            {
                // Zero-overhead direct move for a single subscriber (works for copyable and move-only types)
                active_subs[0]->push(std::move(message));
                return;
            }

            // Multiple subscribers case
            if constexpr (is_cloneable_v<T>)
            {
                for (size_t i = 0; i + 1 < active_subs.size(); ++i)
                {
                    active_subs[i]->push(Cloner<T>::perform(message));
                }
                active_subs.back()->push(std::move(message));
            }
            else
            {
                throw std::runtime_error("Cannot publish move-only type to multiple subscribers without a Cloner/clone mechanism");
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

        /**
         * @brief Publishes a message directly to the topic by moving.
         * @param message The message to publish.
         */
        void Publish(T &&message)
        {
            topic_->Publish(std::move(message));
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
         * @brief Unsubscribes a subscriber from a topic.
         * @tparam T The message type.
         * @param name The name of the topic.
         * @param sub The subscriber to unsubscribe.
         */
        template <typename T>
        void Unsubscribe(const std::string &name, const std::shared_ptr<Subscriber<T>> &sub)
        {
            std::lock_guard<std::mutex> lock(map_mutex_);
            auto it = topics_.find(name);
            if (it != topics_.end())
            {
                auto topic = std::dynamic_pointer_cast<Topic<T>>(it->second);
                if (topic)
                {
                    topic->RemoveSubscriber(sub);
                }
            }
        }

        /**
         * @brief Explicitly removes a topic from the manager.
         * @param name The name of the topic to remove.
         */
        void RemoveTopic(const std::string &name)
        {
            std::lock_guard<std::mutex> lock(map_mutex_);
            topics_.erase(name);
        }

        /**
         * @brief Clears all topics from the manager.
         */
        void ClearTopics()
        {
            std::lock_guard<std::mutex> lock(map_mutex_);
            topics_.clear();
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

        /**
         * @brief Publishes a message to a topic by moving.
         * @tparam T The message type.
         * @param name The name of the topic.
         * @param message The message to publish.
         */
        template <typename T>
        void Publish(const std::string &name, T &&message)
        {
            auto topic = GetOrCreateTopic<typename std::decay<T>::type>(name);
            topic->Publish(std::move(message));
        }
    };

    /**
     * @brief Multiplexer for polling multiple subscribers efficiently.
     *
     * @note On Windows, waiting on more than 64 subscribers simultaneously will
     * trigger a fallback to the Windows Thread Pool API, which carries higher
     * latency and overhead. For optimal performance, distribute subscriptions
     * across multiple Selector or Worker instances (keeping each <= 64).
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
            std::function<bool()> process_all;
            std::function<bool()> is_expired;
            std::atomic<bool> dead{false};
        };

        std::mutex mtx_;
        std::vector<std::shared_ptr<WaitTarget>> targets_;
        bool dirty_ = false;
#ifdef _WIN32
        std::vector<HANDLE> cached_handles_;
#else
        std::vector<struct pollfd> cached_fds_;
#endif

    public:
        /**
         * @brief Adds a subscriber to the selector with a callback.
         * @tparam T The message type.
         * @param sub The subscriber to monitor.
         * @param callback The function to call when a message is received.
         *
         * @warning The callback is captured by value. Ensure that any objects
         * referenced by the callback outlive the Selector or the Subscriber to
         * avoid dangling references.
         */
        template <typename T, typename F>
        void Add(std::shared_ptr<Subscriber<T>> sub, F &&callback)
        {
            auto target = std::make_shared<WaitTarget>();
#ifdef _WIN32
            target->handle = sub->GetWaitHandle();
#else
            target->fd = sub->GetWaitFD();
#endif
            std::weak_ptr<Subscriber<T>> weak_sub = sub;
            target->is_expired = [weak_sub]() -> bool
            {
                return weak_sub.expired();
            };
            target->process_all = [weak_sub, callback = std::forward<F>(callback)]() -> bool
            {
                auto locked_sub = weak_sub.lock();
                if (!locked_sub)
                    return false; // Subscriber is dead

                int max_process = 100; // Limit processing to prevent starvation
                while (max_process-- > 0)
                {
                    auto msg = locked_sub->try_receive();
                    if (!msg)
                        break;
                    callback(std::move(*msg));
                }
                return true; // Still alive
            };

            std::lock_guard<std::mutex> lock(mtx_);
            targets_.push_back(std::move(target));
            dirty_ = true;
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
            int timeout_ms = 0;
            if (timeout >= std::chrono::milliseconds(std::numeric_limits<int>::max()))
            {
                timeout_ms = std::numeric_limits<int>::max();
            }
            else if (timeout < std::chrono::duration<Rep, Period>::zero())
            {
                timeout_ms = -1; // Indefinite wait
            }
            else
            {
                timeout_ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count());
                if (timeout_ms == 0 && timeout > std::chrono::duration<Rep, Period>::zero())
                {
                    timeout_ms = 1; // Round up sub-millisecond positive values to prevent zero-timeout busy loops
                }
            }

            std::vector<std::shared_ptr<WaitTarget>> local_targets;
#ifdef _WIN32
            std::vector<HANDLE> handles;
#else
            std::vector<struct pollfd> fds;
#endif

            {
                std::unique_lock<std::mutex> lock(mtx_);

                // Pre-pass: check for expired subscribers to prevent dead locks / busy loops on invalid fds/handles
                bool expired_found = false;
                for (auto &t : targets_)
                {
                    if (t->is_expired && t->is_expired())
                    {
                        t->dead = true;
                        expired_found = true;
                    }
                }
                if (expired_found)
                {
                    dirty_ = true;
                }

                if (dirty_)
                {
                    auto it = targets_.begin();
                    while (it != targets_.end())
                    {
                        if ((*it)->dead)
                            it = targets_.erase(it);
                        else
                            ++it;
                    }

#ifdef _WIN32
                    cached_handles_.clear();
                    for (const auto &t : targets_)
                        cached_handles_.push_back(t->handle);
#else
                    cached_fds_.clear();
                    for (const auto &t : targets_)
                        cached_fds_.push_back({t->fd, POLLIN, 0});
#endif
                    dirty_ = false;
                }

                if (targets_.empty())
                {
                    lock.unlock();
                    if (timeout_ms > 0)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
                    }
                    else if (timeout_ms < 0)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    }
                    return false;
                }

                local_targets = targets_;
#ifdef _WIN32
                handles = cached_handles_;
#else
                fds = cached_fds_;
#endif
            }

#ifdef _WIN32
            if (handles.size() <= MAXIMUM_WAIT_OBJECTS)
            {
                DWORD result = WaitForMultipleObjects(
                    static_cast<DWORD>(handles.size()), handles.data(), FALSE, static_cast<DWORD>(timeout_ms));

                if (result >= WAIT_OBJECT_0 && result < WAIT_OBJECT_0 + handles.size())
                {
                    bool any_processed = false;
                    for (size_t i = 0; i < handles.size(); ++i)
                    {
                        if (WaitForSingleObject(handles[i], 0) == WAIT_OBJECT_0)
                        {
                            if (!local_targets[i]->process_all())
                            {
                                local_targets[i]->dead = true;
                                std::lock_guard<std::mutex> lock(mtx_);
                                dirty_ = true;
                            }
                            any_processed = true;
                        }
                    }
                    return any_processed;
                }
                return false;
            }
            else
            {
                // [NOTE] Windows > 64 Handles Fallback:
                // When waiting on more than 64 handles, we fallback to the Windows Thread Pool API
                // to avoid busy-waiting. This carries higher latency and overhead. For optimal
                // performance on Windows, distribute subscriptions across multiple Worker instances
                // to keep each under the 64-handle limit.
                HANDLE wakeup_event = CreateEvent(NULL, TRUE, FALSE, NULL);
                if (!wakeup_event)
                    throw std::runtime_error("Failed to create Windows Event for fallback threadpool registration");

                std::vector<HANDLE> wait_handles(handles.size(), NULL);

                for (size_t i = 0; i < handles.size(); ++i)
                {
                    if (!RegisterWaitForSingleObject(&wait_handles[i], handles[i], &detail::SelectorWaitCallback, wakeup_event, INFINITE, WT_EXECUTEONLYONCE))
                    {
                        for (size_t j = 0; j < i; ++j)
                        {
                            if (wait_handles[j])
                            {
                                UnregisterWaitEx(wait_handles[j], INVALID_HANDLE_VALUE);
                            }
                        }
                        CloseHandle(wakeup_event);
                        throw std::runtime_error("RegisterWaitForSingleObject failed");
                    }
                }

                WaitForSingleObject(wakeup_event, static_cast<DWORD>(timeout_ms));

                for (size_t i = 0; i < wait_handles.size(); ++i)
                {
                    if (wait_handles[i])
                    {
                        UnregisterWaitEx(wait_handles[i], INVALID_HANDLE_VALUE);
                    }
                }
                CloseHandle(wakeup_event);

                bool any_processed = false;
                for (size_t i = 0; i < handles.size(); ++i)
                {
                    if (WaitForSingleObject(handles[i], 0) == WAIT_OBJECT_0)
                    {
                        if (!local_targets[i]->process_all())
                        {
                            local_targets[i]->dead = true;
                            std::lock_guard<std::mutex> lock(mtx_);
                            dirty_ = true;
                        }
                        any_processed = true;
                    }
                }
                return any_processed;
            }
#else
            int result = poll(fds.data(), fds.size(), static_cast<int>(timeout_ms));
            if (result > 0)
            {
                bool processed = false;
                for (size_t i = 0; i < fds.size(); ++i)
                {
                    if (fds[i].revents & (POLLIN | POLLERR | POLLHUP | POLLNVAL))
                    {
                        if (!local_targets[i]->process_all())
                        {
                            local_targets[i]->dead = true;
                            std::lock_guard<std::mutex> lock(mtx_);
                            dirty_ = true;
                        }
                        processed = true;
                    }
                }
                return processed;
            }
            return false;
#endif
        }
    };

    /**
     * @brief A background worker that automatically polls a selector.
     *
     * @note On Windows, limiting a Worker to a maximum of 64 subscriptions is
     * recommended for optimal performance. Exceeding this limit invokes the
     * Thread Pool API fallback, which may increase processing latency.
     */
    class Worker
    {
    private:
        std::atomic<bool> running_{false};
        std::thread thread_;
        Selector selector_;

        std::mutex worker_mutex_;
        std::function<void()> tick_callback_;
        std::chrono::milliseconds timeout_{50};

        void ThreadLoop()
        {
            while (running_.load(std::memory_order_relaxed))
            {
                std::chrono::milliseconds timeout;
                std::function<void()> callback;
                {
                    std::lock_guard<std::mutex> lock(worker_mutex_);
                    timeout = timeout_;
                    callback = tick_callback_;
                }

                selector_.WaitFor(timeout);

                if (callback)
                    callback();
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
        template <typename T, typename F>
        void AddSubscription(std::shared_ptr<Subscriber<T>> sub, F &&callback)
        {
            selector_.Add(sub, std::forward<F>(callback));
        }

        /**
         * @brief Sets a callback to be invoked periodically (on every tick).
         * @param timeout The maximum duration to wait between ticks.
         * @param callback The function to call on tick.
         */
        void SetTickCallback(std::chrono::milliseconds timeout, std::function<void()> callback)
        {
            std::lock_guard<std::mutex> lock(worker_mutex_);
            timeout_ = timeout;
            tick_callback_ = std::move(callback);
        }

        /**
         * @brief Starts the background worker thread.
         */
        void Start()
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
        void Stop()
        {
            if (running_.exchange(false))
            {
                if (thread_.joinable())
                {
                    if (std::this_thread::get_id() == thread_.get_id())
                    {
                        thread_.detach();
                    }
                    else
                    {
                        thread_.join();
                    }
                }
            }
        }
    };
}
