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
  bool OptimisticTransaction::AtomicOptcheckForWritesInSamePeriod(int* stableVal) {
    DBImpl* db_impl = static_cast_with_check<DBImpl, DB>(db_->GetRootDB());
    *stableVal = db_impl->asynch_counter->stable_value(); // the timestamp is equal to (*stableVal +1)
    for (auto iter = txnKeysSorted.begin(); iter != txnKeysSorted.end(); ++iter) {
      if (!dbimpl_->unstablePeriodCache.append(*iter, *stableVal + 1)) {
        return true;
       }
    }
    return false;
  }

  void OptimisticTransaction::Optwrite_text_to_log_file(int txnID, int stableVal, int commitWait) {
    std::ofstream log_file("opt_log_file.txt", std::ios_base::out | std::ios_base::app);
    std::string keys;
    for (auto iter = txnKeysSorted.begin(); iter != txnKeysSorted.end(); ++iter) {
      keys += (*iter);
      keys +=  " ";
    }

    log_file << commitWait << " " << txnID << " " << stableVal << ", " << keys << "\n";
  }
  Status OptimisticTransaction::Prepare() {
    return Status::InvalidArgument(
        "Two phase commit not supported for optimistic transactions.");
  }

  
  Status OptimisticTransaction::Commit() {
    // Set up callback which will call CheckTransactionForConflicts() to
    // check whether this transaction is safe to be committed.
    OptimisticTransactionCallback callback(this);

    DBImpl* db_impl = static_cast_with_check<DBImpl, DB>(db_->GetRootDB());

    int commitWait = 0;
    int stableVal = -1;
    if (AtomicOptcheckForWritesInSamePeriod(&stableVal)) {
          commitWait = 1;
          std::this_thread::sleep_for(std::chrono::milliseconds((db_impl->asynch_counter->expected_time())));
        
          // after our commit-wait we try to 'check and update' the keys
          // If this fail again, probably another txn do this first so we will
          // fail. Our updates/appends are not rolled back but this is not
          // considered to be a problem since we assume workloads with very
          // little conflicts.
          if (AtomicOptcheckForWritesInSamePeriod(&stableVal))
              return Status::Busy();
    }

    
    GetWriteBatch()->GetWriteBatch()->counterTimestamp = stableVal + 1;
    Status s = db_impl->WriteWithCallback(
              write_options_, GetWriteBatch()->GetWriteBatch(), &callback);
    /* Status s = db_impl->WriteWithCallback(
         write_options_, GetWriteBatch()->GetWriteBatch(), &callback); */

    if (s.ok()) {
      // Optwrite_text_to_log_file(GetID(), (stableVal+1), commitWait);
      std::atomic_fetch_add(&(db_impl->num_commit), 1);
      if (commitWait)
        std::atomic_fetch_add(&(db_impl->num_commit_waits), 1);
      struct return_values* ret = new return_values;
      // db_impl->asynch_counter->increment(ret, stableVal+1);
      db_impl->asynch_counter->increment(ret);
      delete ret;
      Clear();
    }
    return s;
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
