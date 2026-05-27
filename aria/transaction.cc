#include <algorithm>
#include <cstdio>
#include <cstring>

#include "include/common.hh"
#include "include/transaction.hh"

TxnExecutor::TxnExecutor(int thid, Result *sres) : thid_(thid), sres_(sres) {
  read_set_.reserve(FLAGS_max_ope);
  write_set_.reserve(FLAGS_max_ope);
  pro_set_.reserve(FLAGS_max_ope);

  txn_id_ = 0;
  epoch_ = 0;
  waw_ = false;
  war_ = false;
  raw_ = false;
  read_only_ = false;

  genStringRepeatedNumber(write_val_, VAL_SIZE, thid);
}

void TxnExecutor::begin() {
  status_ = TransactionStatus::kInFlight;
  read_set_.clear();
  write_set_.clear();
  waw_ = false;
  war_ = false;
  raw_ = false;
  read_only_ = true;
}

void TxnExecutor::abort() {
  read_set_.clear();
  write_set_.clear();
}

// Phase 1: Read snapshot
// Aria reads from a consistent snapshot (no locks needed).
// Simply copy the current value of the tuple.
void TxnExecutor::read(uint64_t key) {
  // read-own-writes or re-read
  if (searchReadSet(key) || searchWriteSet(key)) return;

  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#else
  tuple = get_tuple(Table, key);
#endif

  // Aria reads from a snapshot: just copy the value.
  // No lock-spin needed because all writes from the previous batch
  // are already committed before this batch starts.
  memcpy(return_val_, tuple->val_, VAL_SIZE);

  read_set_.emplace_back(key, tuple, return_val_);
}

void TxnExecutor::write(uint64_t key, std::string_view val) {
  if (searchWriteSet(key)) return;

  read_only_ = false;

  Tuple *tuple;
  ReadElement<Tuple> *re = searchReadSet(key);
  if (re) {
    tuple = re->rcdptr_;
  } else {
#if MASSTREE_USE
    tuple = MT.get_value(key);
#else
    tuple = get_tuple(Table, key);
#endif
  }

  write_set_.emplace_back(key, tuple, val);
}

// Phase 2: Reservation
// Update metadata for each key in read/write sets via CAS.
// The metadata stores the MINIMUM non-zero txn_id (= highest priority).
// Lower txn_id = higher priority in Aria's deterministic total order.
void TxnExecutor::reserve() {
  // Read-only optimization: skip reservation entirely
  if (FLAGS_aria_read_only_opt && is_read_only()) return;

  // Reserve reads
  for (auto &r : read_set_) {
    reserve_read(r.rcdptr_->metadata_, epoch_, txn_id_);
  }

  // Reserve writes
  for (auto &w : write_set_) {
    reserve_write(w.rcdptr_->metadata_, epoch_, txn_id_);
  }
}

// Phase 3: Analyze dependency (conflict detection)
// Metadata stores the MINIMUM non-zero txn_id (highest priority).
// A conflict exists when a higher-priority (lower-ID) transaction
// has reserved a key that the current transaction also accesses.
// Condition: stored_id < txn_id_ && stored_id != 0
void TxnExecutor::analyze_dependency() {
  if (FLAGS_aria_read_only_opt && is_read_only()) return;

  // Check read set for RAW conflicts:
  // A higher-priority transaction wants to write a key we read.
  for (auto &r : read_set_) {
    uint64_t meta = r.rcdptr_->metadata_.load(std::memory_order_acquire);
    // Stale epoch means no reservation this batch
    if (AriaMetadata::get_epoch(meta) != epoch_) continue;
    uint64_t wts = AriaMetadata::get_wts(meta);
    if (wts != 0 && wts < txn_id_) {
      raw_ = true;
      break;
    }
  }

  // Check write set for WAR and WAW conflicts:
  for (auto &w : write_set_) {
    uint64_t meta = w.rcdptr_->metadata_.load(std::memory_order_acquire);
    // Stale epoch means no reservation this batch
    if (AriaMetadata::get_epoch(meta) != epoch_) continue;
    uint64_t rts = AriaMetadata::get_rts(meta);
    uint64_t wts = AriaMetadata::get_wts(meta);

    // WAW: a higher-priority transaction also wants to write this key
    if (wts != 0 && wts < txn_id_) {
      waw_ = true;
    }
    // WAR: a higher-priority transaction wants to read a key we write
    if (rts != 0 && rts < txn_id_) {
      war_ = true;
    }
  }
}

// Phase 3: Commit - apply writes to tuples
void TxnExecutor::commit() {
  for (auto &w : write_set_) {
    if (w.get_val_length() == 0) {
      memcpy(w.rcdptr_->val_, write_val_, VAL_SIZE);
    } else {
      memcpy(w.rcdptr_->val_, w.get_val_ptr(), w.get_val_length());
    }
  }

  status_ = TransactionStatus::kCommitted;
  read_set_.clear();
  write_set_.clear();
}

ReadElement<Tuple> *TxnExecutor::searchReadSet(uint64_t key) {
  for (auto &r : read_set_) {
    if (r.key_ == key) return &r;
  }
  return nullptr;
}

WriteElement<Tuple> *TxnExecutor::searchWriteSet(uint64_t key) {
  for (auto &w : write_set_) {
    if (w.key_ == key) return &w;
  }
  return nullptr;
}

// CAS-based reservation: store the MINIMUM non-zero txn_id in rts.
// Uses epoch-based lazy reset: if the stored epoch is stale, overwrite
// entirely with the new epoch (no need for a separate reset pass).
void TxnExecutor::reserve_read(std::atomic<uint64_t> &meta,
                                uint64_t epoch, uint64_t txn_id) {
  uint64_t expected = meta.load(std::memory_order_acquire);
  for (;;) {
    uint64_t cur_epoch = AriaMetadata::get_epoch(expected);
    uint64_t desired;

    if (epoch > cur_epoch) {
      // Stale epoch: no valid reservation yet this batch.
      // Overwrite with new epoch and our txn_id as rts, wts=0.
      desired = AriaMetadata::make(epoch, txn_id, 0);
    } else {
      // Same epoch: use min-ID logic
      uint64_t cur_rts = AriaMetadata::get_rts(expected);
      if (cur_rts != 0 && cur_rts < txn_id) {
        break;  // Higher priority already reserved
      }
      uint64_t cur_wts = AriaMetadata::get_wts(expected);
      desired = AriaMetadata::make(epoch, txn_id, cur_wts);
    }

    if (meta.compare_exchange_weak(expected, desired,
                                   std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      break;
    }
    // CAS failed, expected is updated; retry
  }
}

// CAS-based reservation: store the MINIMUM non-zero txn_id in wts.
// Uses epoch-based lazy reset (same as reserve_read).
void TxnExecutor::reserve_write(std::atomic<uint64_t> &meta,
                                 uint64_t epoch, uint64_t txn_id) {
  uint64_t expected = meta.load(std::memory_order_acquire);
  for (;;) {
    uint64_t cur_epoch = AriaMetadata::get_epoch(expected);
    uint64_t desired;

    if (epoch > cur_epoch) {
      // Stale epoch: overwrite with new epoch and our txn_id as wts, rts=0.
      desired = AriaMetadata::make(epoch, 0, txn_id);
    } else {
      // Same epoch: use min-ID logic
      uint64_t cur_wts = AriaMetadata::get_wts(expected);
      if (cur_wts != 0 && cur_wts < txn_id) {
        break;  // Higher priority already reserved
      }
      uint64_t cur_rts = AriaMetadata::get_rts(expected);
      desired = AriaMetadata::make(epoch, cur_rts, txn_id);
    }

    if (meta.compare_exchange_weak(expected, desired,
                                   std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      break;
    }
  }
}
