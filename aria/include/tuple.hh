#pragma once

#include <cstdint>
#include <atomic>

#include "../../include/cache_line_size.hh"

// Aria metadata: 64-bit word
// [epoch:24 | rts:20 | wts:20]
// rts/wts store transaction IDs (not timestamps).
// Within the same epoch, higher ID = later in total order.
struct AriaMetadata {
  static constexpr uint64_t WTS_BITS = 20;
  static constexpr uint64_t RTS_BITS = 20;
  static constexpr uint64_t EPOCH_BITS = 24;

  static constexpr uint64_t WTS_MASK = (1ULL << WTS_BITS) - 1;
  static constexpr uint64_t RTS_MASK = (1ULL << RTS_BITS) - 1;
  static constexpr uint64_t EPOCH_MASK = (1ULL << EPOCH_BITS) - 1;

  static constexpr uint64_t WTS_OFFSET = 0;
  static constexpr uint64_t RTS_OFFSET = WTS_BITS;
  static constexpr uint64_t EPOCH_OFFSET = WTS_BITS + RTS_BITS;

  static uint64_t get_wts(uint64_t v) {
    return (v >> WTS_OFFSET) & WTS_MASK;
  }

  static uint64_t get_rts(uint64_t v) {
    return (v >> RTS_OFFSET) & RTS_MASK;
  }

  static uint64_t get_epoch(uint64_t v) {
    return (v >> EPOCH_OFFSET) & EPOCH_MASK;
  }

  static uint64_t make(uint64_t epoch, uint64_t rts, uint64_t wts) {
    return ((epoch & EPOCH_MASK) << EPOCH_OFFSET) |
           ((rts & RTS_MASK) << RTS_OFFSET) |
           ((wts & WTS_MASK) << WTS_OFFSET);
  }
};

class Tuple {
public:
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> metadata_;
  char val_[VAL_SIZE];
};
