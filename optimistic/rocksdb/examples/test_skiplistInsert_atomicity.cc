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


#define OPNUM 5 // number of PUT operations (updates)
#define TESTDURATION 1000000 // number of transactions (must be huge)
using namespace rocksdb;


std::string kDBPath = "/tmp/rocksdb_transaction_example";
static std::atomic<int> mycounter;

void client_function(OptimisticTransactionDB* txn_db, int id) {
    WriteOptions write_options;
    int commits = 0;
    while (TESTDURATION > mycounter) {
        Transaction* txn = txn_db->BeginTransaction(write_options);
        assert(txn);
        Status s;

        uint64_t k;
        char key[100];
        k = rand()% 50000 + 1;
        for (int i = 0; i < 10; i++) {
            k = k + i;
            snprintf(key, sizeof(key), "%d", k);
            s = txn->Put(key, std::to_string(id));
        }

        s = txn->Commit();
        commits++;
        if (commits%1000 == 0 && commits > 0)
            std::cout << "Thread id: " << id << " (commits : " << commits << ")\n";
        delete txn;
        std::atomic_fetch_add(&mycounter, 1);
    }
}

int main() {
    // open DB
    Options options;
    options.create_if_missing = true;
    //DB* db;
    OptimisticTransactionDB* txn_db;
    mycounter = 0;
    Status s = OptimisticTransactionDB::Open(options, kDBPath, &txn_db);
    assert(s.ok());
    //db = txn_db->GetBaseDB();

    int new_clients = 0, CLIENTS = 4;
    std::vector<std::thread*> clients;

    while (new_clients < CLIENTS) {
        clients.push_back(new std::thread(client_function, txn_db, new_clients));
        new_clients++;
    }

    for (auto& client : clients) {
        client->join();
    }

    for (unsigned i = 0; i < clients.size(); i++){
        delete clients[i];
    }


    // Cleanup
    delete txn_db;
    std::cout << "Bomb the db.. time to crash\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    
    DestroyDB(kDBPath, options);
    return 0;
}

#endif  // ROCKSDB_LITE
