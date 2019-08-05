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
#include "rocksdb/utilities/transaction_db.h"
#include <iostream>
#include <ctime>   
#include <stdlib.h>     /* srand, rand */

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

void client_function(TransactionDB* txn_db, int id) {
  WriteOptions write_options;

  Transaction* txn = txn_db->BeginTransaction(write_options);
  assert(txn);
  uint64_t k;

  Status s; 

  if (id % 3 == 0) {
    k = rand() % 10 + 1;
    char key[100];
    snprintf(key, sizeof(int), "%d", k);
    s = txn->Put(key, std::to_string(id));
  }
  else if (id % 3 == 1) {
    k = rand() % 10 + 1;
    char key[100];
    snprintf(key, sizeof(int), "%d", k);
    s = txn->Put(key, std::to_string(id));
  }
  else {
    k = rand() % 10 + 1;
    char key[100];
    snprintf(key, sizeof(int), "%d", k);
    s = txn->Put(key, std::to_string(id));
  }
  s = txn->Commit();
  assert(s.ok());
  delete txn;
}

int main() {
  /* open DB */
  Options options;
  TransactionDBOptions txn_db_options;
  options.create_if_missing = true;
  TransactionDB* txn_db;
  txn_db_options.transaction_lock_timeout = 1000000;
  int new_clients = 0, CLIENTS = 60;
  Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
  assert(s.ok());

  std::vector<std::thread*> clients;
  std::vector<std::thread*> clients2;
  std::cout<<"================================================================================\n";
  while(new_clients < CLIENTS) {
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

  new_clients = 0;
  while(new_clients < CLIENTS) {
    clients2.push_back(new std::thread(client_function, txn_db, new_clients));
    new_clients++;
  }

  for (auto& client : clients2) {
    client->join();
  }

  for (unsigned i = 0; i < clients2.size(); i++){
    delete clients2[i];
  }


  delete txn_db;
  // DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
