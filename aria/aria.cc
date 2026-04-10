#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <atomic>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "../include/atomic_wrapper.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#if MASSTREE_USE
#include "../include/masstree_wrapper.hh"
#endif
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"

using namespace std;

// Spin barrier for inter-phase synchronization.
// All worker threads must reach the barrier before any can proceed.
class SpinBarrier {
public:
  explicit SpinBarrier(size_t n) : num_threads_(n), count_(0), generation_(0) {}

  void wait() {
    uint64_t gen = generation_.load(std::memory_order_acquire);
    if (count_.fetch_add(1, std::memory_order_acq_rel) == num_threads_ - 1) {
      // Last thread to arrive: reset counter and advance generation
      count_.store(0, std::memory_order_relaxed);
      generation_.fetch_add(1, std::memory_order_release);
    } else {
      // Wait until generation advances
      while (generation_.load(std::memory_order_acquire) == gen) {
        _mm_pause();
      }
    }
  }

private:
  size_t num_threads_;
  std::atomic<size_t> count_;
  std::atomic<uint64_t> generation_;
};

// Global barrier shared by all worker threads
static SpinBarrier *global_barrier = nullptr;


void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
  Result &myres = std::ref(AriaResult[thid]);
  Xoroshiro128Plus rnd;
  rnd.init();
  TxnExecutor trans(thid, &myres);
  FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);

#ifdef Linux
  setThreadAffinity(thid);
#endif

#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif

  // Per-thread batch of transactions
  const size_t batch_size = FLAGS_batch_size;
  // For retry_aborted: track which transactions in the batch were aborted
  // and should be retried. Store their procedures.
  vector<vector<Procedure>> batch_procedures(batch_size);
  vector<bool> batch_aborted(batch_size, false);
  uint64_t epoch = 1;

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

  // ===== Main batch loop =====
  while (!loadAcquire(quit)) {
    // === Generate batch of transactions ===
    for (size_t b = 0; b < batch_size; ++b) {
      if (FLAGS_retry_aborted && batch_aborted[b]) {
        // Reuse the procedure from the previous batch (retry)
        // batch_procedures[b] already has the procedures
      } else {
        // Generate new transaction
#if PARTITION_TABLE
        makeProcedure(batch_procedures[b], rnd, zipf, FLAGS_tuple_num,
                      FLAGS_max_ope, FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw,
                      FLAGS_ycsb, true, thid, myres);
#else
        makeProcedure(batch_procedures[b], rnd, zipf, FLAGS_tuple_num,
                      FLAGS_max_ope, FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw,
                      FLAGS_ycsb, false, thid, myres);
#endif

#if PROCEDURE_SORT
        sort(batch_procedures[b].begin(), batch_procedures[b].end());
#endif
      }
    }

    if (loadAcquire(quit)) break;

    // Storage for per-transaction read/write sets through phases 1-3
    struct TxnState {
      vector<ReadElement<Tuple>> read_set;
      vector<WriteElement<Tuple>> write_set;
      uint64_t txn_id;
      bool read_only;
    };
    vector<TxnState> batch_state(batch_size);

    // === Phase 1: Read Snapshot ===
    // All threads execute their batch, collecting read/write sets.
    // Reads see a consistent snapshot (all writes from previous batch
    // are committed). Sets are saved for phases 2-3.
    for (size_t b = 0; b < batch_size; ++b) {
      uint64_t txn_id = thid * batch_size + b + 1;

      trans.begin();
      trans.txn_id_ = txn_id;
      trans.pro_set_ = batch_procedures[b];

      for (auto &op : trans.pro_set_) {
        if (op.ope_ == Ope::READ) {
          trans.read(op.key_);
        } else if (op.ope_ == Ope::WRITE) {
          trans.write(op.key_);
        } else if (op.ope_ == Ope::READ_MODIFY_WRITE) {
          trans.read(op.key_);
          trans.write(op.key_);
        }
      }

      // Save read/write sets for phases 2-3
      batch_state[b].read_set = std::move(trans.read_set_);
      batch_state[b].write_set = std::move(trans.write_set_);
      batch_state[b].txn_id = txn_id;
      batch_state[b].read_only = trans.is_read_only();
    }

    // Barrier: all threads must finish Phase 1 before Phase 2
    global_barrier->wait();

    if (loadAcquire(quit)) break;

    // === Phase 2: Reservation ===
    // Each thread reserves its read/write keys in the metadata.
    // Uses saved read/write sets from Phase 1 (no re-execution needed).
    for (size_t b = 0; b < batch_size; ++b) {
      trans.read_set_ = std::move(batch_state[b].read_set);
      trans.write_set_ = std::move(batch_state[b].write_set);
      trans.txn_id_ = batch_state[b].txn_id;
      trans.read_only_ = batch_state[b].read_only;
      trans.epoch_ = epoch;

      trans.reserve();

      // Move sets back for Phase 3
      batch_state[b].read_set = std::move(trans.read_set_);
      batch_state[b].write_set = std::move(trans.write_set_);
    }

    // Barrier: all threads must finish Phase 2 before Phase 3
    global_barrier->wait();

    if (loadAcquire(quit)) break;

    // === Phase 3: Conflict Analysis & Commit ===
    for (size_t b = 0; b < batch_size; ++b) {
      // Restore state into trans for analysis
      trans.read_set_ = std::move(batch_state[b].read_set);
      trans.write_set_ = std::move(batch_state[b].write_set);
      trans.txn_id_ = batch_state[b].txn_id;
      trans.read_only_ = batch_state[b].read_only;
      trans.epoch_ = epoch;
      trans.waw_ = false;
      trans.war_ = false;
      trans.raw_ = false;
      trans.status_ = TransactionStatus::kInFlight;

      // Read-only transactions always commit
      if (FLAGS_aria_read_only_opt && trans.is_read_only()) {
        trans.status_ = TransactionStatus::kCommitted;
        trans.read_set_.clear();
        trans.write_set_.clear();
        storeRelease(myres.local_commit_counts_,
                     loadAcquire(myres.local_commit_counts_) + 1);
        batch_aborted[b] = false;
        continue;
      }

      // Analyze conflicts
      trans.analyze_dependency();

      // Commit decision
      bool do_commit = false;

      if (trans.waw_) {
        // WAW: always abort
        do_commit = false;
      } else if (FLAGS_aria_snapshot_isolation) {
        // SI mode: only abort on WAW (already handled above)
        do_commit = true;
      } else if (FLAGS_aria_reordering) {
        // Reordering optimization: commit if !(WAR && RAW)
        do_commit = !(trans.war_ && trans.raw_);
      } else {
        // Default (Serializability): abort on RAW
        do_commit = !trans.raw_;
      }

      if (do_commit) {
        trans.commit();
        storeRelease(myres.local_commit_counts_,
                     loadAcquire(myres.local_commit_counts_) + 1);
        batch_aborted[b] = false;
      } else {
        trans.abort();
        ++myres.local_abort_counts_;
        if (FLAGS_retry_aborted) {
          batch_aborted[b] = true;
        } else {
          batch_aborted[b] = false;
        }
      }
    }

    // Barrier: all threads must finish Phase 3 before next batch
    global_barrier->wait();

    ++epoch;
  }

  return;
}

int main(int argc, char *argv[]) try {
  gflags::SetUsageMessage("Aria benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  makeDB();

  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();

  // Create barrier for all worker threads
  global_barrier = new SpinBarrier(FLAGS_thread_num);

  std::vector<char> readys(FLAGS_thread_num);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread_num; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit));
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto &th : thv) th.join();

  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    AriaResult[0].addLocalAllResult(AriaResult[i]);
  }
  ShowOptParameters();
  AriaResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime,
                                 FLAGS_thread_num);

  delete global_barrier;

  return 0;
} catch (bad_alloc) {
  ERR;
}
