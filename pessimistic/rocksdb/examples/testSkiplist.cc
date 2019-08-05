// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include <iostream>
#include <thread>

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

void DBget(TransactionDB* txn_db, int* key) {

  Transaction* txn = txn_db->BeginTransaction(WriteOptions());
  assert(txn);
  std::string value;
  Status s = txn->Get(ReadOptions(), "def", &value);
  int tmp_val;
  if (s.ok()) {
    // Found key, parse its value
    tmp_val = std::stoull(value);
  } else if (s.IsNotFound()) {
    tmp_val = 0;
    s = Status::OK();
  }
  // txn->Commit();
  *key = tmp_val;
  s = txn->Put("abc", value);
  txn->Commit();

}

int main() {
  // open DB
  Options options;
  TransactionDBOptions txn_db_options;
  options.create_if_missing = true;
  TransactionDB* txn_db;

  Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
  assert(s.ok());

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  for (int i = 0; i < 10000000; i ++){
    Transaction* txn = txn_db->BeginTransaction(write_options);
    assert(txn);

    // Write a key in this transaction
    uint64_t k = rand() %500;
    char key[100];
    snprintf(key, sizeof(uint64_t), "%d", i);
    s = txn->Put(key, key);
    assert(s.ok());
    s = txn->Commit();

    delete txn;
  }
  int int_value = 0;;
  DBget(txn_db, &int_value);
  // Cleanup
  std::cout << "Bomb RocksDB!\n";
  std::cout << int_value << "\n";
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  delete txn_db;
  DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
