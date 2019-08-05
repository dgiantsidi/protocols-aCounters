#pragma once 
#include <unordered_set>
#include <map>
#include <iostream>
#include <string>
#include <fstream>
#include "hashMap/inc/HashMap.h"
namespace rocksdb {

#define DISTANCE 10 // number of minimum live indexes even if no accessed in the latest unstable period

  class TemporaryCache {
    public:
      CTSL::HashMap<std::string, int> myIndex;
      std::map<int, CTSL::HashMap<std::string, int>*> tableOfIndexes;
      std::map<int, std::atomic<int>> liveIndexesAccesses;
      int indexToBeDeleted;

      std::atomic<int> clearIndex;
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

      TemporaryCache() {
        clearIndex = 0;
        int id;
        if ((id = return_counter_last_stable_value()) == -1) {
        tableOfIndexes[1] = new CTSL::HashMap<std::string, int>;
        liveIndexesAccesses[1].store(0, std::memory_order_seq_cst);
        indexToBeDeleted = 1;
        }
        else {
        tableOfIndexes[id+1] = new CTSL::HashMap<std::string, int>;
        liveIndexesAccesses[id+1].store(0, std::memory_order_seq_cst);
        indexToBeDeleted = id+1;
        }
      };

      ~ TemporaryCache() {
        std::cout << "[Clean up remaining Indexes]\n";
        for (auto it = tableOfIndexes.begin(); it != tableOfIndexes.end(); it++) {
          delete it->second;
          tableOfIndexes.erase(it);
        }
      };

      bool append(std::string key, int epoch) {
        while(clearIndex.load() != 0) {};
        std::shared_lock<std::shared_timed_mutex> lock(_mutex);

        /* returns false if duplicate keys
         * and returns true if key does not
         * exist or exist in a past timestamp/epoch
         */
        if (tableOfIndexes.find(epoch) == tableOfIndexes.end())
          return false;
        liveIndexesAccesses[epoch].store(1, std::memory_order_seq_cst);
        bool success = tableOfIndexes.find(epoch)->second->insert(key, epoch);
        return success;

      };

      void clear(std::atomic<int>* stableVal, int incremented_val) {
        std::atomic_fetch_add(&clearIndex, 1);
        std::unique_lock<std::shared_timed_mutex> lock(_mutex);
        /**
         * This also "garbage-collect" old Indexes that have not been
         * accessed lately (last epoch). We choose a distance to
         * accomodate long transactions.
         */
        if (indexToBeDeleted + DISTANCE < incremented_val) {
          if (liveIndexesAccesses[indexToBeDeleted] == 0){
            // std::cout << "[Delete Index matched to timestamp: " << indexToBeDeleted<< "]\n";
            delete tableOfIndexes.find(indexToBeDeleted)->second;
            tableOfIndexes.erase(indexToBeDeleted);
            indexToBeDeleted++;
            // std::cout << "[Delete Index matched to timestamp: " << indexToBeDeleted<< " Successful]\n";
          }
        }

        for (int i = indexToBeDeleted; i < incremented_val; i++) {
          liveIndexesAccesses[i].store(0, std::memory_order_seq_cst);
        }
        tableOfIndexes[incremented_val+1] = new CTSL::HashMap<std::string, int>;
        liveIndexesAccesses[incremented_val + 1].store(0, std::memory_order_seq_cst);
        // std::cout << "[Index for timestamp: " << incremented_val + 1 << " created]\n";
        stableVal->store(incremented_val, std::memory_order_seq_cst);

        // logging the counter value to the file would no longer needed when integrating to SPEICHER
        std::ofstream log_file("counter_file.txt", std::ios_base::out | std::ios_base::app);
        log_file << stableVal->load() << "\n";

        std::atomic_fetch_sub(&clearIndex, 1);
      };


      bool searchKey(std::string key) {
        int value;
        while(clearIndex.load() != 0) {};

        if (!myIndex.find(key, value)) {
          return false;
        }                                               
        return true;
      }

      mutable std::shared_timed_mutex _mutex;
  };
}
