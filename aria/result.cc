#include "include/result.hh"
#include "include/common.hh"

#include "../include/cache_line_size.hh"
#include "../include/result.hh"

alignas(CACHE_LINE_SIZE) std::vector<Result> AriaResult;

void initResult() { AriaResult.resize(FLAGS_thread_num); }
