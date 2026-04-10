#include <cstdio>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

#include "include/common.hh"
#include "include/transaction.hh"
#include "include/tuple.hh"
#include "include/util.hh"

#include "../include/cache_line_size.hh"
#include "../include/config.hh"
#include "../include/debug.hh"
#if MASSTREE_USE
#include "../include/masstree_wrapper.hh"
#endif
#include "../include/random.hh"
#include "../include/util.hh"

using std::cout;
using std::endl;

void chkArg() {
  displayParameter();

  if (FLAGS_rratio > 100) {
    ERR;
  }

  if (FLAGS_zipf_skew >= 1) {
    cout << "FLAGS_zipf_skew must be 0 ~ 0.999..." << endl;
    ERR;
  }
}

void displayDB() {
  Tuple *tuple;
  for (unsigned int i = 0; i < FLAGS_tuple_num; ++i) {
    tuple = &Table[i];
    cout << "------------------------------" << endl;
    cout << "key: " << i << endl;
    cout << "val: " << tuple->val_ << endl;
    cout << "metadata: " << tuple->metadata_.load() << endl;
    cout << endl;
  }
}

void displayParameter() {
  cout << "#FLAGS_clocks_per_us:\t" << FLAGS_clocks_per_us << endl;
  cout << "#FLAGS_extime:\t\t" << FLAGS_extime << endl;
  cout << "#FLAGS_max_ope:\t\t" << FLAGS_max_ope << endl;
  cout << "#FLAGS_rmw:\t\t" << FLAGS_rmw << endl;
  cout << "#FLAGS_rratio:\t\t" << FLAGS_rratio << endl;
  cout << "#FLAGS_thread_num:\t" << FLAGS_thread_num << endl;
  cout << "#FLAGS_tuple_num:\t" << FLAGS_tuple_num << endl;
  cout << "#FLAGS_ycsb:\t\t" << FLAGS_ycsb << endl;
  cout << "#FLAGS_zipf_skew:\t" << FLAGS_zipf_skew << endl;
  cout << "#FLAGS_batch_size:\t" << FLAGS_batch_size << endl;
  cout << "#FLAGS_retry_aborted:\t" << FLAGS_retry_aborted << endl;
  cout << "#FLAGS_aria_reordering:\t" << FLAGS_aria_reordering << endl;
  cout << "#FLAGS_aria_snapshot_isolation:\t" << FLAGS_aria_snapshot_isolation
       << endl;
  cout << "#FLAGS_aria_read_only_opt:\t" << FLAGS_aria_read_only_opt << endl;
}

void partTableInit([[maybe_unused]] size_t thid, uint64_t start, uint64_t end) {
#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(thid);
#endif

  for (auto i = start; i <= end; ++i) {
    Tuple *tmp;
    tmp = &Table[i];
    tmp->metadata_.store(0, std::memory_order_relaxed);
    tmp->val_[0] = 'a';
    tmp->val_[1] = '\0';

#if MASSTREE_USE
    MT.insert_value(i, tmp);
#endif
  }
}

void makeDB() {
  if (posix_memalign((void **)&Table, PAGE_SIZE,
                     (FLAGS_tuple_num) * sizeof(Tuple)) != 0)
    ERR;

  size_t maxthread = decideParallelBuildNumber(FLAGS_tuple_num);

  std::vector<std::thread> thv;
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(partTableInit, i, i * (FLAGS_tuple_num / maxthread),
                     (i + 1) * (FLAGS_tuple_num / maxthread) - 1);
  for (auto &th : thv) th.join();
}

void ShowOptParameters() {
  cout << "#ShowOptParameters()"
       << ": ADD_ANALYSIS " << ADD_ANALYSIS << ": KEY_SIZE " << KEY_SIZE
       << ": MASSTREE_USE " << MASSTREE_USE << ": PARTITION_TABLE "
       << PARTITION_TABLE << ": PROCEDURE_SORT " << PROCEDURE_SORT
       << ": VAL_SIZE " << VAL_SIZE << endl;
}
