**pessimistic/rocksdb/**: includes the code for our proposed protocol under Pessimistic Concurrency Control. Please do not rely on this version for optimistic case since it may be the original or a stale version.

`mylib/counter.h` : The emulated ACM interface.

`mylib/temporaryCache.h` :  The implementation of our Index. Specifically we use a `rocksdb::InlineSkiplist` instance. We provide two key comparators (Numerical and Bytewise) and we only use the `BytewiseComparator` to support all key types.

`utilities/transactions/pessimistic_transaction.h`, `utilities/transactions/pessimistic_transaction.cc` 
Our protocol's implementation.
- `CommitWithoutPrepareInternalWithRollbackProtection(int timestamp);` : Passes the txn's timestamp to lower software layers.
- `checkForWritesInSamePeriod();` : Returns true if at least one key in `GetTrackedKeys()` is already in the Skip List.
- `putTxnKeysIntoTempCache();` : Updates the Skip List with the committed keys.

`db/db_impl.h` : We extend the logic of the db to 'inject' our counter and Index objects as well as the recovery algorithm.

`db/db_impl.h`, `db/db_impl_write.cc` : Logging Records along with timestamps.
- `MergeBatchesOfSameEpoch(const WriteThread::WriteGroup& write_group);` : Instead of merging all records into a single batch we create a list with all sub-batches (individual to each txn). That way, we preserve the individual timestamps. Since non-conflicting transactions can be processed in parallel merging may concatenate transactions with different timestamps.
- `WriteToWALWithTimestamps(const std::map<int, WriteBatch*>* groupBatches,
                           log::Writer* log_writer, uint64_t* log_used,
                           uint64_t* log_size);` : Our implementation to store transactions' write batches (local buffers) along with their acquired timestamps.

`db/log_reader.h`, `db/log_reader.cc` : WAL reader modification.
- `ReadRecord(Slice* record, std::string* scratch, int* timestamp, WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords);` Reconstructs a write-batch from WAL.
- `ReadPhysicalRecord(Slice* result, size_t* drop_size, int* previous_val);` : Reads a physical record from WAL and returns its type.

`db/log_writer.h`, `db/log_writer.cc` : WAL writer modification.
- `AddRecord(const Slice& slice, const int timestamp);`: Takes a write batch and appends it in the WAL. may be splitted to one or multiple WAL records.
- `EmitPhysicalRecord(RecordType type, const char* ptr, size_t length,
                               int timestamp);`: Writes a single physical record to the WAL.


`db/db_impl_open.cc` : Recovery Algorithm.
- `RecoverLogFilesWithRollbackProtection(const std::vector<uint64_t>& log_numbers,
             SequenceNumber* next_sequence, bool read_only);` We implement the recovery algorithm with respect to the records' timestamps.





**optimistic/rocksdb/**: includes the code for our proposed protocol under Optimistic Concurrency Control. 

`mylib/counter.h` : The emulated ACM interface (similar to optimistic).

`mylib/temporaryCache.h` :  The implementation of the Index. We use a concurrent thread-safe hash map. Keys are arbitrary strings.  The implementation of the concurrent thread-safe hash map can be found in `mylib/hashMap/` directory.

`utilities/transactions/optimistic_transaction.h`, `utilities/transactions/optimistic_transaction.cc` 
Our protocol's implementation.
- `AtomicOptcheckForWritesInSamePeriod(int *stableVal);;` : Passes the txn's timestamp to lower software layers.
- `TryLock();` : It is normally invoked at every transactional PUT  to record key for doing the conflict checking later in the commit-phase. We exploit this function to keep the keys ordered for when searching and appending in the Index.

The rest of the files have been modified similarly to the pessimistic case.

