#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "../../include/string.hh"
#include "aria_op_element.hh"
#include "common.hh"
#include "tuple.hh"

enum class TransactionStatus : uint8_t {
  kInFlight,
  kCommitted,
  kAborted,
};

class TxnExecutor {
public:
  std::vector<ReadElement<Tuple>> read_set_;
  std::vector<WriteElement<Tuple>> write_set_;
  std::vector<Procedure> pro_set_;

  TransactionStatus status_;
  unsigned int thid_;
  uint64_t txn_id_;  // deterministic ID within the batch
  uint64_t epoch_;   // current batch epoch (for lazy metadata reset)
  Result *sres_;

  // conflict flags (set during analyze_dependency)
  bool waw_;
  bool war_;
  bool raw_;
  bool read_only_;

  char write_val_[VAL_SIZE];
  char return_val_[VAL_SIZE];

  TxnExecutor(int thid, Result *sres);

  void begin();
  void abort();

  // Phase 1: Read snapshot - read values without locking
  void read(uint64_t key);
  void write(uint64_t key, std::string_view val = "");

  // Phase 2: Reservation - reserve reads/writes in metadata via CAS
  void reserve();

  // Phase 3: Conflict analysis and commit
  void analyze_dependency();
  void commit();

  Tuple *get_tuple(Tuple *table, uint64_t key) { return &table[key]; }
  ReadElement<Tuple> *searchReadSet(uint64_t key);
  WriteElement<Tuple> *searchWriteSet(uint64_t key);

  bool is_read_only() const { return read_only_; }

private:
  static void reserve_read(std::atomic<uint64_t> &meta, uint64_t epoch, uint64_t txn_id);
  static void reserve_write(std::atomic<uint64_t> &meta, uint64_t epoch, uint64_t txn_id);
};
