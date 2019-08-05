//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/optimistic_transaction.h"

#include <string>
#include "fstream"
#include "db/column_family.h"
#include "db/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

  struct WriteOptions;

  OptimisticTransaction::OptimisticTransaction(
      OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
      const OptimisticTransactionOptions& txn_options)
    : TransactionBaseImpl(txn_db->GetBaseDB(), write_options), txn_db_(txn_db) {
      Initialize(txn_options);
    }

  void OptimisticTransaction::Initialize(
      const OptimisticTransactionOptions& txn_options) {
    if (txn_options.set_snapshot) {
      SetSnapshot();
    }
  }

  void OptimisticTransaction::Reinitialize(
      OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
      const OptimisticTransactionOptions& txn_options) {
    TransactionBaseImpl::Reinitialize(txn_db->GetBaseDB(), write_options);
    Initialize(txn_options);
  }

  OptimisticTransaction::~OptimisticTransaction() {}

  void OptimisticTransaction::Clear() { TransactionBaseImpl::Clear(); }

  /**
   * this is my custom 'atomic check & append'
   */
  bool OptimisticTransaction::OptcheckForWritesInSamePeriod() {
    for (auto iter = txnKeysSorted.begin(); iter != txnKeysSorted.end(); ++iter) {
      if (!dbimpl_->unstablePeriodCache.append(*iter)) {
        return true;
       }
    }     
    return false;
  }


  void OptimisticTransaction::OptputTxnKeysIntoTempCache() {
    for (auto iter = txnKeysSorted.begin(); iter != txnKeysSorted.end(); ++iter) {
      dbimpl_->unstablePeriodCache.append(*iter);
    }  
  }

  void OptimisticTransaction::Optwrite_text_to_log_file (int txnID, int stableVal) {
    std::ofstream log_file("opt_log_file.txt", std::ios_base::out | std::ios_base::app);
    std::string keys;
    // const auto& tracked_keys = GetTrackedKeys();
    // const auto tracked_keys_cf = tracked_keys.find(0); 
    // for (auto iter = tracked_keys_cf->second.begin(); iter != tracked_keys_cf->second.end(); ++iter) {
    for (auto iter = txnKeysSorted.begin(); iter != txnKeysSorted.end(); ++iter) {
      // keys += (*iter).first;
      keys += (*iter);
      keys +=  " ";
    }

    log_file << txnID << " " << stableVal << ", " << keys << "\n";
  }
  Status OptimisticTransaction::Prepare() {
    return Status::InvalidArgument(
        "Two phase commit not supported for optimistic transactions.");
  }

  // REQUIRES: Must be called with the secondaryMutex_ held.
  bool OptimisticTransaction::alreadyLockedKeys() {
    const auto& tracked_keys = GetTrackedKeys();
    const auto tracked_keys_cf = tracked_keys.find(0); 
    std::string key;

    for (auto iter = tracked_keys_cf->second.begin(); iter != tracked_keys_cf->second.end(); ++iter) {
      key = (*iter).first;
      if (txn_db_->secondaryCache.find(key) != txn_db_->secondaryCache.end())
        return true;
     }
    return false;
  }

  // REQUIRES: Must be called with the secondaryMutex_ held.
  void OptimisticTransaction::lockKeys() {
    const auto& tracked_keys = GetTrackedKeys();
    const auto tracked_keys_cf = tracked_keys.find(0); 
    std::string key;
      for (auto iter = tracked_keys_cf->second.begin(); iter != tracked_keys_cf->second.end(); ++iter) {
        key = (*iter).first;
        // (dimitra): atomically lock keys
        txn_db_->secondaryCache.insert(std::make_pair(key, -1));
      }

  }

  bool OptimisticTransaction::TrylockKeys() {
   /**
    * const auto& tracked_keys = GetTrackedKeys();
    * const auto tracked_keys_cf = tracked_keys.find(0); 
    * std::string key;
    */
    txn_db_->secondaryMutex_.lock();

    if (alreadyLockedKeys()) {
      lockKeys();
      txn_db_->secondaryMutex_.unlock();
    }
    else {
      /**
       * We reach this point because common keys are found in the secondary
       * cache. We will try locking in a `timeout` period.
       * Afterwards, if our attempt to lock the keys is unsuccessful, we will abort.
       *
       * Since we are in the Optimistic Concurrency control and thus, no locks
       * are held, a transaction in this case can only succeed if the previous
       * transaction (the transaction due to which the keys are locked) has
       * already committed but havent cleared the locks yet. In any other case,
       * this transaction will abort. We assume that the time distance a
       * transaction needs to update the Skiplist and clear the locks is at most
       * 20 ms. If after this period we continue to be unable to lock the keys:
       *  1) either the ongoing transaction commit-waits (so guaranteed fail)
       *  2) either another transaction entered commit-phase
       */
      int64_t timeout = 20; // milliseconds
      txn_db_->secondaryMutex_.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
      txn_db_->secondaryMutex_.lock();
      if(alreadyLockedKeys()) {
          txn_db_->secondaryMutex_.unlock();
          return false;
      }
      lockKeys();
      txn_db_->secondaryMutex_.unlock();
    }
   return true;
  }

  void OptimisticTransaction::unlockKeys() {
    const auto& tracked_keys = GetTrackedKeys();
    const auto tracked_keys_cf = tracked_keys.find(0); 
    std::string key;

    txn_db_->secondaryMutex_.lock();
    
    for (auto iter = tracked_keys_cf->second.begin(); iter != tracked_keys_cf->second.end(); ++iter) {
      key = (*iter).first;
      txn_db_->secondaryCache.erase(key);
    }
    txn_db_->secondaryMutex_.unlock();
  }

  Status OptimisticTransaction::Commit() {
    // Set up callback which will call CheckTransactionForConflicts() to
    // check whether this transaction is safe to be committed.
    OptimisticTransactionCallback callback(this);

    DBImpl* db_impl = static_cast_with_check<DBImpl, DB>(db_->GetRootDB());



    
    Status s = db_impl->WriteWithCallback(
        write_options_, GetWriteBatch()->GetWriteBatch(), &callback);

    if (s.ok()) {
      Clear();
    }
    return s;

    // (dimitra): use the second cache to lock the keys
    /*
    if (TrylockKeys()) {
    if (OptcheckForWritesInSamePeriod()) {
          std::atomic_fetch_add(&(db_impl->num_commit_waits), 1);
          std::this_thread::sleep_for(std::chrono::milliseconds((db_impl->asynch_counter->expected_time())));
    }

    
    struct return_values* ret = new return_values;
    db_impl->asynch_counter->increment(ret);
    delete ret;
    
    Optwrite_text_to_log_file(GetID(), db_impl->asynch_counter->stable_value());
    
    Status s = db_impl->WriteWithCallback(
        write_options_, GetWriteBatch()->GetWriteBatch(), &callback);

    if (s.ok()) {
      std::atomic_fetch_add(&(db_impl->num_commit), 1);
      OptputTxnKeysIntoTempCache();
      unlockKeys();
      Clear();
    }
    
    return s;
    }
    return Status::Busy();*/
  }

  Status OptimisticTransaction::Rollback() {
    Clear();
    return Status::OK();
  }

  // Record this key so that we can check it for conflicts at commit time.
  //
  // 'exclusive' is unused for OptimisticTransaction.
  Status OptimisticTransaction::TryLock(ColumnFamilyHandle* column_family,
      const Slice& key, bool read_only,
      bool exclusive, const bool do_validate,
      const bool assume_tracked) {
    assert(!assume_tracked);  // not supported
    (void)assume_tracked;
    if (!do_validate) {
      return Status::OK();
    }
    uint32_t cfh_id = GetColumnFamilyID(column_family);

    SetSnapshotIfNeeded();

    SequenceNumber seq;
    if (snapshot_) {
      seq = snapshot_->GetSequenceNumber();
    } else {
      seq = db_->GetLatestSequenceNumber();
    }

    std::string key_str = key.ToString();

    // we append a tracked key in the sorted container
    txnKeysSorted.insert(key_str);

    TrackKey(cfh_id, key_str, seq, read_only, exclusive);

    // Always return OK. Confilct checking will happen at commit time.
    return Status::OK();
  }

  // Returns OK if it is safe to commit this transaction.  Returns Status::Busy
  // if there are read or write conflicts that would prevent us from committing OR
  // if we can not determine whether there would be any such conflicts.
  //
  // Should only be called on writer thread in order to avoid any race conditions
  // in detecting write conflicts.
  Status OptimisticTransaction::CheckTransactionForConflicts(DB* db) {
    Status result;

    auto db_impl = static_cast_with_check<DBImpl, DB>(db);

    // Since we are on the write thread and do not want to block other writers,
    // we will do a cache-only conflict check.  This can result in TryAgain
    // getting returned if there is not sufficient memtable history to check
    // for conflicts.
    return TransactionUtil::CheckKeysForConflicts(db_impl, GetTrackedKeys(),
        true /* cache_only */);
  }

  Status OptimisticTransaction::SetName(const TransactionName& /* unused */) {
    return Status::InvalidArgument("Optimistic transactions cannot be named.");
  }

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
