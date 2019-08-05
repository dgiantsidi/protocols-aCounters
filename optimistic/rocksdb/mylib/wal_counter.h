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
#define WAL_STABILITY_TIME_INTERVAL 60 

#define WAL_CHECK_TIME_INTERVAL 0   // further delay (if necessary)


struct wal_counter_values {
  std::atomic<int> cur_stable_val;
  long expected_time;
  std::atomic<int> incremented_val;
};

static std::atomic<int> wal_stop;
static std::atomic<int> wal_store_in_progress;
static std::shared_timed_mutex _wal_counterMutex;

class WALCounter {
  public:

    int return_walcounter_last_stable_value() {
      std::ifstream ifs;
      std::string line, prev;
      int id;

      ifs.open ("wal_counter.txt", std::ifstream::in);
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

    WALCounter() {
      val = new wal_counter_values;
      /**
       * Initialize it to zero or retrieve it from persistent
       * storage (useful in case of a system crush).
       */
      int id = 0;
      if ((id = return_walcounter_last_stable_value()) != -1)
        val->cur_stable_val = id;     
      else 
        val->cur_stable_val = 0;

      val->expected_time = WAL_CHECK_TIME_INTERVAL + WAL_STABILITY_TIME_INTERVAL; // msec
      val->incremented_val = (id != -1) ? id : 0;
      wal_stop = 0;
      wal_store_in_progress = 0;
      /**
       * This dedicated thread will continuously inspect
       * for any changes to our counter.
       */
      agent = std::thread(wal_inspect_changes, val);
    }


    /**
     * wal_counter and timestamp_counter in our current implementation are not
     * necesserily synchronized concurrently. This is NOT a problem since we do
     * not use this information for our protocols. When integrating with
     * SPEICHER this will have already been implemented.
     */
    static void wal_inspect_changes(struct wal_counter_values* val) {
      int stable_val, incremented_val;
      while (true && !wal_stop.load()) {
        stable_val = val->cur_stable_val.load();
        incremented_val = val->incremented_val.load();
        if (incremented_val != stable_val) {
          std::atomic_fetch_add(&(wal_store_in_progress), 1);
          {
            std::unique_lock<std::shared_timed_mutex> lock(_wal_counterMutex);
            std::this_thread::sleep_for(std::chrono::milliseconds(WAL_CHECK_TIME_INTERVAL));
            /**
             * To ensure that the Index flushing and counter synchronization happen atomically 
             * we embedd the synchronization operation (writing to the file) inside the 
             * 'clear' utility. 
             */
            val->cur_stable_val.store(incremented_val, std::memory_order_seq_cst);
            std::ofstream log_file("wal_counter.txt", std::ios_base::out | std::ios_base::app);
            log_file << val->cur_stable_val.load() << "\n";
            std::atomic_fetch_sub(&(wal_store_in_progress), 1);
          }
          /**
           * We use this timer to emulate the unstable period. During this period the locks are not held
           * and therefore the asynchronous counter can be increased.
           */
          std::this_thread::sleep_for(std::chrono::milliseconds(WAL_STABILITY_TIME_INTERVAL));
        }
      }
    }


    /* Returns the current stable value */
    int stable_value() {
      while (wal_store_in_progress.load()) {};
      std::shared_lock<std::shared_timed_mutex> lock(_wal_counterMutex); 
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
     * Increments the asynchronous counter. The 'while' should be considered as a flag and it 
     * is needed to ensure fairness in case of multiple threads/clients 
     * continuously invoke the increment method while the agent-thread needs to update 
     * its value.
     */
    void increment() {
      while (wal_store_in_progress.load()) {};
      std::shared_lock<std::shared_timed_mutex> lock(_wal_counterMutex); 
      std::atomic_fetch_add(&val->incremented_val, 1);
    }

    ~WALCounter() {
      std::cout << "[WAL counter ready to be deleted]\n";
      std::atomic_fetch_add(&wal_stop, 1);
      agent.join();
      std::cout << "[WAL counter deleted]\n";
      delete val;
    };

  private:
    struct wal_counter_values *val;
    std::thread agent;
    // bool first_increment = true;
};
