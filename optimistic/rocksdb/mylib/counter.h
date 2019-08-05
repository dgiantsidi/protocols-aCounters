#pragma once
#include <chrono>
#include <thread>
#include <iostream>
#include <mutex>
#include <time.h>
#include <atomic>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include "temporaryCache.h"
#include <shared_mutex>

#define STABILITY_TIME_INTERVAL 0 // time to stabilize the asynch counter value (in milliseconds)
#define CHECK_TIME_INTERVAL 60    // period of uncertainty (asynch counter is incremented without


struct counter_values {
    std::atomic<int> cur_stable_val;
    long expected_time;
    std::atomic<int> incremented_val;
};

struct return_values {
    int stable_value;
    long expected_time;
    int incremented_value;
};

static std::atomic<int> stop;
static std::atomic<int> store_in_progress;
static rocksdb::TemporaryCache *keysTempcache;
static std::shared_timed_mutex _counterMutex;

class AsynchCounter {
    public:
        AsynchCounter() {
            val = new counter_values;
            /**
             * initialize it to zero or retrieve it from persistent
             * storage (useful in case of a system crush)
             */
            val->cur_stable_val = 0;
            val->expected_time = CHECK_TIME_INTERVAL + STABILITY_TIME_INTERVAL; // msec
            val->incremented_val = 0;
            stop = 0;
            store_in_progress = 0;

            /**
             * this dedicated thread will inspect
             * for any changes to our counter
             */
            agent = std::thread(inspect_changes, val);
        }


        static void inspect_changes(struct counter_values* val) {
            int incremented_val, stable_val;

            while (true && !stop.load()) {
                stable_val = val->cur_stable_val.load();
                incremented_val = val->incremented_val.load();
                if (stable_val != incremented_val) {
                    std::atomic_fetch_add(&(store_in_progress), 1);

                    {
                        std::unique_lock<std::shared_timed_mutex> lock(_counterMutex);
                        std::this_thread::sleep_for(std::chrono::milliseconds(STABILITY_TIME_INTERVAL));
                        keysTempcache->clear(&(val->cur_stable_val), incremented_val);
                        std::atomic_fetch_sub(&(store_in_progress), 1);
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_TIME_INTERVAL)); 
                }
            }
        }

        void copy_values(struct return_values *ret) {
            ret->stable_value = static_cast<int>(val->cur_stable_val.load());
            ret->expected_time = val->expected_time;
            ret->incremented_value = static_cast<int>(val->incremented_val.load());
        }

        /* Returns the current stable value */
        int stable_value() {
            while (store_in_progress.load()) {};
            std::shared_lock<std::shared_timed_mutex> lock(_counterMutex); 
            int ret_val = static_cast<int>(val->cur_stable_val.load());
            return ret_val;
        }

        int current_value() {
            return static_cast<int>(val->incremented_val.load());
        }

        int expected_time() {
            return val->expected_time;
        }

        void increment(struct return_values *ret, int originTimestamp) {
            while (store_in_progress.load()) {};

            std::shared_lock<std::shared_timed_mutex> lock(_counterMutex);
            if (originTimestamp <= val->cur_stable_val) {
                copy_values(ret);
                return;
            }
            if (val->incremented_val != val->cur_stable_val + 1)
                val->incremented_val = val->cur_stable_val + 1;
            copy_values(ret);
        }

        void increment(struct return_values *ret) {
            while (store_in_progress.load()) {};
            std::shared_lock<std::shared_timed_mutex> lock(_counterMutex); 
            std::atomic_fetch_add(&val->incremented_val, 1);
            copy_values(ret);
        }

        void setCachePtr(rocksdb::TemporaryCache *c) {
            keysTempcache = c;
        };

        ~AsynchCounter() {
            std::atomic_fetch_add(&stop, 1);
            agent.join();
            delete val;
        };

    private:
        struct counter_values *val;
        std::thread agent;
};
