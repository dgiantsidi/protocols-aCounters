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
#define A_STABILITY_TIME_INTERVAL 60 

#define A_CHECK_TIME_INTERVAL 0   // further delay (if necessary)


struct A_counter_values {
    std::atomic<int> cur_stable_val;
    long expected_time;
    std::atomic<int> incremented_val;
};

struct A_return_values {
    int stable_value;
    long expected_time; 
    /* In our implementation, 'expected_time' equals to the unstable period */
    int incremented_value;
};

static std::atomic<int> A_stop;
static std::atomic<int> A_store_in_progress;
static rocksdb::TemporaryCache *_keysTempcache;

static std::shared_timed_mutex A_counterMutex;

class AsynchCounters {
    public:
        int return_counter_last_stable_value() {
            std::ifstream ifs;
            std::string line, prev;
            int id;

            ifs.open ("counter_file.txt", std::ifstream::in);
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

        AsynchCounters() {
            val = new A_counter_values;
            wal_val = new A_counter_values;
            /**
             * Initialize it to zero or retrieve it from persistent
             * storage (useful in case of a system crush).
             */
            int id = 0;
            if ((id = return_counter_last_stable_value()) != -1)
                val->cur_stable_val = id;
            else
                val->cur_stable_val = 0;

            val->expected_time = A_CHECK_TIME_INTERVAL + A_STABILITY_TIME_INTERVAL; // msec
            val->incremented_val = (id != -1) ? id : 0;
            A_stop = 0;
            A_store_in_progress = 0;
            wal_val->cur_stable_val = 0;
            wal_val->incremented_val = 0;
            /**
             * This dedicated thread will continuously inspect
             * for any changes to our counter.
             */
            vals = (struct A_counter_values**) malloc(sizeof(struct A_counter_values*) *2);
            vals[0] = val;
            vals[1] = wal_val;
            agent = std::thread(_inspect_changes, vals);
        }


        static void _inspect_changes(struct A_counter_values** vals) {
            struct A_counter_values *val = vals[0];
            struct A_counter_values *wal_val = vals[1];

            int stable_val, incremented_val;
            while (true && !A_stop.load()) {
                stable_val = val->cur_stable_val.load();
                incremented_val = val->incremented_val.load();
                if (incremented_val != stable_val) {
                    std::atomic_fetch_add(&(A_store_in_progress), 1);
                    {
                        std::unique_lock<std::shared_timed_mutex> lock(A_counterMutex);
                        std::this_thread::sleep_for(std::chrono::milliseconds(A_CHECK_TIME_INTERVAL));
                        /**
                         * To ensure that the Index flushing and counter synchronization happen atomically 
                         * we embedd the synchronization operation (writing to the file) inside the 
                         * 'clear' utility. 
                         */
                        _keysTempcache->clear(&(val->cur_stable_val), incremented_val);

                        if (wal_val->cur_stable_val.load() != wal_val->incremented_val.load()) {
                            wal_val->cur_stable_val.store(wal_val->incremented_val.load(), std::memory_order_seq_cst);
                            std::ofstream log_file("wal_counter1.txt", std::ios_base::out | std::ios_base::app);
                            log_file << wal_val->cur_stable_val.load() << "\n";
                        }
                        std::atomic_fetch_sub(&(A_store_in_progress), 1);

                    }
                    /**
                     * We use this timer to emulate the unstable period. During this period the locks are not held
                     * and therefore the asynchronous counter can be increased.
                     */
                    std::this_thread::sleep_for(std::chrono::milliseconds(A_STABILITY_TIME_INTERVAL));
                }
                else if (wal_val->cur_stable_val.load() != wal_val->incremented_val.load()) {
                    std::atomic_fetch_add(&(A_store_in_progress), 1);
                    {
                        std::unique_lock<std::shared_timed_mutex> lock(A_counterMutex);
                        std::this_thread::sleep_for(std::chrono::milliseconds(A_CHECK_TIME_INTERVAL));
                        /**
                         * To ensure that the Index flushing and counter synchronization happen atomically 
                         * we embedd the synchronization operation (writing to the file) inside the 
                         * 'clear' utility. 
                         */
                        wal_val->cur_stable_val.store(wal_val->incremented_val.load(), std::memory_order_seq_cst);
                        std::ofstream log_file("wal_counter1.txt", std::ios_base::out | std::ios_base::app);
                        log_file << wal_val->cur_stable_val.load() << "\n";
                        std::atomic_fetch_sub(&(A_store_in_progress), 1);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(A_STABILITY_TIME_INTERVAL));
                }
            }
        }

        void copy_values(struct A_return_values *ret) {
            ret->stable_value = static_cast<int>(val->cur_stable_val.load());
            ret->expected_time = val->expected_time;
            ret->incremented_value = static_cast<int>(val->incremented_val.load());
        }

        /* Returns the current stable value */
        int stable_value() {
            while (A_store_in_progress.load()) {};
            std::shared_lock<std::shared_timed_mutex> lock(A_counterMutex); 
            int ret_val = static_cast<int>(val->cur_stable_val.load());
            return ret_val;
        }

        int current_value() {
            return static_cast<int>(val->incremented_val.load());
        }

        int wal_current_value() {
            return static_cast<int>(wal_val->incremented_val.load());
        }
        int expected_time() {
            return val->expected_time;
        }


        /**
         * This is NEVER used (has been developped during our experimentation).
         * Our proposed increment method. An invocation from an already-stable timestamp will not further
         * increase the counter.
         */
        void increment(struct A_return_values *ret, int originTimestamp) {
            while (A_store_in_progress.load()) {};

            std::shared_lock<std::shared_timed_mutex> lock(A_counterMutex); 
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
        void increment(struct A_return_values *ret) {
            while (A_store_in_progress.load()) {};
            std::shared_lock<std::shared_timed_mutex> lock(A_counterMutex); 
            std::atomic_fetch_add(&val->incremented_val, 1);
            copy_values(ret);
        }

        void incrementWAL() {
            while (A_store_in_progress.load()) {};
            std::shared_lock<std::shared_timed_mutex> lock(A_counterMutex); 
            std::atomic_fetch_add(&wal_val->incremented_val, 1);
        }

        void setCachePtr(rocksdb::TemporaryCache *c) {
            _keysTempcache = c;
        };

        ~AsynchCounters() {
            std::cout << "[TimestampCounter ready to deleted]\n";
            std::atomic_fetch_add(&A_stop, 1);
            agent.join();
            std::cout << "[TimestampCounter deleted]\n";
            delete val;
            delete wal_val;
            free(vals);
            std::cout << "All counters destroyed\n";
        };

    private:
        struct A_counter_values *val; // timestamp-counter
        struct A_counter_values *wal_val; // WAL-counter
        struct A_counter_values** vals;
        std::thread agent;
};
