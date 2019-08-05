#pragma once 
#include "../../../memtable/inlineskiplist.h"
#include "../../../util/concurrent_arena.h"
#include <unordered_set>
#include <iostream>
#include <string>
#include "db/memtable.h"
#include "rocksdb/comparator.h"
#include <shared_mutex>
#include <iostream>
#include <fstream>
#include "util/coding.h"
#include <assert.h>

namespace rocksdb {

  /* NumberComparator requires that the keys are uint64_t */
  typedef uint64_t CachedKey;

  static CachedKey Decode(const char* b) {
    return (uint64_t) atoi(b);
  }

  struct MockNumberComparator {
    typedef CachedKey DecodedType;

    static DecodedType decode_key(const char* b) {
      return Decode(b);
    }

    int operator()(const char* a, const char* b) const {
      if (Decode(a) < Decode(b)) {
        return -1;
      } else if (Decode(a) > Decode(b)) {
        return +1;
      } else {
        return 0;
      }
    }

    int operator()(const char* a, const DecodedType b) const {
      if (Decode(a) < b) {
        return -1;
      } else if (Decode(a) > b) {
        return +1;
      } else {
        return 0;
      }
    }
  };


  static Slice DecodeSlice(const char* a) {
    Slice k1 = Slice(a, strlen(a));
    return k1;
  }

  /** 
   * We implement the interface for a Skip List comparator using RocksDB's BytewiseComparator().
   * keys are plain bytes expressed as std::string and are casted to char arrays. The comparator 
   * is responsible for ordering.
   */
  struct ByteComparator {
    typedef Slice DecodedType;

    const Comparator* _compare = BytewiseComparator();

    static DecodedType decode_key(const char* b) {
      return DecodeSlice(b);
    }

    int operator()(const char* a, const char* b) const {
      Slice k1 = Slice(a, strlen(a));
      Slice k2 = Slice(b, strlen(b));
      int ret = _compare->Compare(k1, k2);
      return ret;
    }

    int operator()(const char* a, const DecodedType b) const {
      Slice k1 = Slice(a, strlen(a));
      int ret =  _compare->Compare(k1, b);
      return ret;
    }
  };


  class TemporaryCache {
    public:
      /* The mutex for this temporary cache */
      mutable std::shared_timed_mutex _cacheMutex; 
      /* thread-safe allocator that allocates nodes in the Skip List */
      ConcurrentArena arena; 
      /* the cmp is used to order the keys in the Skip List */
      ByteComparator cmp;      

      InlineSkipList<ByteComparator> *myIndex;

      std::atomic<int> clearIndex; // used as flag to ensure fairness

      TemporaryCache() {
        myIndex = new InlineSkipList<ByteComparator>(cmp, &arena);
        clearIndex = 0;
      };

      ~ TemporaryCache() {
        delete myIndex;
      };

      /*
       * Requires: should never be called concurrently with something 
       * that is compared equal to key.
       */
      bool append(std::string key) {
        while(clearIndex.load() != 0) {};

        std::shared_lock<std::shared_timed_mutex> lock(_cacheMutex); 

        char* buf = nullptr;
        uint64_t s = 1;
        uint32_t key_size = static_cast<uint32_t>(key.size());
        uint32_t internal_key_size = key_size + 8;

        const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size;


        /* Allocates a Node in the Skip List */
        buf = myIndex->AllocateKey(encoded_len);


        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);

        p += key_size;
        uint64_t packed = PackSequenceAndType(s, kTypeValue);
        EncodeFixed64(p, packed);


        bool success = myIndex->InsertConcurrently(buf);

        /* key should be present in the Skip List */
        assert(_searchKeyInternal(key));

        /* No duplicate keys should
         * exist (due to commit-wait).
         * This assertation only makes sense when Pessimistic Transactions are
         * issued with commit-wait enabled.
         */
        assert(success);

        return success; // returns always true (if false is return then duplicate key exists)

      };



      void clear(std::atomic<int>* stableVal, int incremented_val) {
        std::atomic_fetch_add(&clearIndex, 1); // this acts like a flag!

        std::unique_lock<std::shared_timed_mutex> lock(_cacheMutex);

        InlineSkipList<ByteComparator> *new_index = new InlineSkipList<ByteComparator>(cmp, &arena);

        InlineSkipList<ByteComparator> *temp = myIndex;
        myIndex = new_index;
        delete temp;

        stableVal->store(incremented_val, std::memory_order_seq_cst);

        // this is for logging -> not used when integrating with SPEICHER
        std::ofstream log_file("counter_file.txt", std::ios_base::out | std::ios_base::app);
        log_file << stableVal->load() << "\n";


        // std::cout << "[Clear Skip List: current stable value : " << stableVal->load() << "]\n";

        std::atomic_fetch_sub(&clearIndex, 1);
      };


      /**
       * In our system 'malloc' is thread-safe. (see 'man malloc').
       * This function is only for testing (assertation in append method).
       * Assumes that shared_lock is held.
       */
      bool _searchKeyInternal(std::string key) {
        char* buf = nullptr;

        uint64_t s = 1;
        uint32_t key_size = static_cast<uint32_t>(key.size());
        uint32_t internal_key_size = key_size + 8;

        const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size;

        /* 'malloc' is thread-safe in our tested-system */
        buf = (char *) malloc(encoded_len);

        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);

        p += key_size;
        uint64_t packed = PackSequenceAndType(s, kTypeValue);
        EncodeFixed64(p, packed);

        if (!myIndex->Contains(buf)) {
          free(buf);
          return false;
        }
        free(buf);
        return true;
      }

      bool searchKey(std::string key) {

        while(clearIndex.load() != 0) {};

        std::shared_lock<std::shared_timed_mutex> lock(_cacheMutex); 
        char* buf = nullptr;

        uint64_t s = 1;
        uint32_t key_size = static_cast<uint32_t>(key.size());
        uint32_t internal_key_size = key_size + 8;

        const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size;

        buf = (char *) malloc(encoded_len);


        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);

        p += key_size;
        uint64_t packed = PackSequenceAndType(s, kTypeValue);
        EncodeFixed64(p, packed);

        if (!myIndex->Contains(buf)) {
          free(buf);
          return false;
        }
        free(buf);
        return true;

      };
  };
}
