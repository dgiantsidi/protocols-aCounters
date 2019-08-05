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
#include <atomic>

#define TXNS 1000000

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_transaction_example";


std::atomic<int> counter;

void client_function(TransactionDB* txn_db, int id) {
    WriteOptions write_options;
    char key[100];
    uint64_t k, base, commits = 0;
    // std::string keys;

    while (counter.load() < TXNS) {
        Transaction* txn = txn_db->BeginTransaction(write_options);
        assert(txn);
        base = rand()%1000000 + 1;
        Status s; 
        for (int i = 0; i < 1000; i++) {
            k = base + i;
            snprintf(key, sizeof(key), "%d", k);
            txn->Put(key, std::to_string(id));
            // keys += key;
            // keys += " ";
        }
        s = txn->Commit();
        commits++;
        if (commits%1000 == 0 && commits >0)
            std::cout << "Thread id: " << id << " (commits: " << commits <<  ")\n";
        std::atomic_fetch_add(&counter,1);
        delete txn;
    }

}

int main() {
    // open DB
    Options options;
    TransactionDBOptions txn_db_options;
    options.create_if_missing = true;
    TransactionDB* txn_db;
    txn_db_options.transaction_lock_timeout = 1000;
    int new_clients = 0, CLIENTS = 8;
    Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
    assert(s.ok());
    counter = 0;
    std::vector<std::thread*> clients;
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

    std::cout << "All threads joined, time to crash\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    // Cleanup
    delete txn_db;
    DestroyDB(kDBPath, options);
    return 0;
}

#endif  // ROCKSDB_LITE
