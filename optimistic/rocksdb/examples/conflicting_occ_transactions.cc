// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <chrono>
#include <thread>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
#include <iostream>
#include <ctime>   
#include <stdlib.h>     /* srand, rand */

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

void client_function(OptimisticTransactionDB* txn_db, int id) {
  WriteOptions write_options;

  for (int i = 0; i < 1; i++){
  Transaction* txn = txn_db->BeginTransaction(write_options);
  assert(txn);
  Status s;

  uint64_t k;
  char key[100];
  k = rand() % 10 + 1;
  snprintf(key, sizeof(key), "%d", k);
  s = txn->Put(key, std::to_string(id));

  s = txn->Commit();
  if (s.ok())
    std::cout<< " Thread "<< std::to_string(id) << " committed \n";
  delete txn;
  }
}

int main() {
  // open DB
  Options options;
  options.create_if_missing = true;
  DB* db;
  OptimisticTransactionDB* txn_db;



  Status s = OptimisticTransactionDB::Open(options, kDBPath, &txn_db);
  assert(s.ok());
  db = txn_db->GetBaseDB();

  int new_clients = 0, CLIENTS = 12;
  std::vector<std::thread*> clients;
  std::vector<std::thread*> clients2;
  std::cout<<"================================================================================\n";
  while(new_clients < CLIENTS) {
    if (new_clients == 1) 
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

    clients.push_back(new std::thread(client_function, txn_db, new_clients));
    new_clients++;
  }

  for (auto& client : clients) {
    client->join();
  }
  for (unsigned i = 0; i < clients.size(); i++){
    delete clients[i];
  }

  WriteOptions write_options;

  Transaction* txn = txn_db->BeginTransaction(write_options);
  uint64_t k = 3;
  char key[100];
  snprintf(key, sizeof(key), "%d", k);
  s = txn->Put(key, "abc");
  assert(s.ok());
  s = txn->Commit();
  assert(s.ok());
  delete txn;

  std::this_thread::sleep_for(std::chrono::milliseconds(3000));

  new_clients= 0;
  while(new_clients < CLIENTS) {
    if (new_clients == 1) 
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    clients2.push_back(new std::thread(client_function, txn_db, new_clients));
    new_clients++;
  }

  for (auto& client : clients2) {
    client->join();
  }

  for (unsigned i = 0; i < clients2.size(); i++){
    delete clients2[i];
  }

  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
