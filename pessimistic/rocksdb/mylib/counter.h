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

/* time to stabilize the most recent asynchronous (software) counter value (in milliseconds) */
#define STABILITY_TIME_INTERVAL 60 

#define CHECK_TIME_INTERVAL 0   // further delay (if necessary)


struct counter_values {
    std::atomic<int> cur_stable_val;
    long expected_time;
    std::atomic<int> incremented_val;
};

struct return_values {
    int stable_value;
    long expected_time; 
    /* In our implementation, 'expected_time' equals to the unstable period */
    int incremented_value;
};

static std::atomic<int> stop;
static std::atomic<int> store_in_progress;
static rocksdb::TemporaryCache *keysTempcache;

static std::shared_timed_mutex _counterMutex;

class AsynchCounter {
    public:
        int return_counter_last_stable_value() {
            std::ifstream ifs;
            std::string line, prev;
            int id;

            ifs.open ("counter.txt", std::ifstream::in);
            while (std::getline(ifs, line)) {
                prev = line;
            }

            if (prev.size() != 0) {
                id =  std::stoi(prev);
            }
            else {
                id = -1;
            }
            ifs.close();

            return id;
        }

        AsynchCounter() {
            val = new counter_values;
            /**
             * Initialize it to zero or retrieve it from persistent
             * storage (useful in case of a system crush).
             */
            int id = 0;
            if ((id = return_counter_last_stable_value()) != -1)
                val->cur_stable_val = id;
            else
                val->cur_stable_val = 0;
            val->expected_time = CHECK_TIME_INTERVAL + STABILITY_TIME_INTERVAL; // msec
            val->incremented_val = (id != -1) ? id : 0;
            stop = 0;
            store_in_progress = 0;
            /**
             * This dedicated thread will continuously inspect
             * for any changes to our counter.
             */
            agent = std::thread(inspect_changes, val);
        }


        static void inspect_changes(struct counter_values* val) {
            int stable_val, incremented_val;
            while (true && !stop.load()) {
                stable_val = val->cur_stable_val.load();
                incremented_val = val->incremented_val.load();
                if (incremented_val != stable_val) {
                    std::atomic_fetch_add(&(store_in_progress), 1);
                    {
                        std::unique_lock<std::shared_timed_mutex> lock(_counterMutex);
                        std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_TIME_INTERVAL));
                        /**
                         * To ensure that the Index flushing and counter synchronization happen atomically 
                         * we embedd the synchronization operation (writing to the file) inside the 
                         * 'clear' utility. 
                         */
                        keysTempcache->clear(&(val->cur_stable_val), incremented_val);
                        std::atomic_fetch_sub(&(store_in_progress), 1);
                    }
                    /**
                     * We use this timer to emulate the unstable period. During this period the locks are not held
                     * and therefore the asynchronous counter can be increased.
                     */
                    std::this_thread::sleep_for(std::chrono::milliseconds(STABILITY_TIME_INTERVAL));
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


        /**
         * This is NEVER used (has been developped during our experimentation).
         * Our proposed increment method. An invocation from an already-stable timestamp will not further
         * increase the counter.
         */
        void increment(struct return_values *ret, int originTimestamp) {
            while (store_in_progress.load()) {};

            std::shared_lock<std::shared_timed_mutex> lock(_counterMutex); 
            if (originTimestamp <= val->cur_stable_val) {
                copy_values(ret);
                return;
            }
            if (val->incremented_val != (val->cur_stable_val + 1))
                val->incremented_val = val->cur_stable_val + 1;
            copy_values(ret);
        }

        /** 
         * Increments the asynchronous counter. The 'while' should be considered as a flag and it 
         * is needed to ensure fairness in case of multiple threads/clients 
         * continuously invoke the increment method while the agent-thread needs to update 
         * its value.
         */
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
            std::cout << "[TimestampCounter ready to deleted]\n";
            std::atomic_fetch_add(&stop, 1);
            agent.join();
            std::cout << "[TimestampCounter deleted]\n";
            delete val;
        };

    private:
        struct counter_values *val;
        std::thread agent;
};
