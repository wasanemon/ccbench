#pragma once

#include <cstring>
#include <memory>
#include <string_view>

template <typename T>
class ReadElement {
public:
  uint64_t key_;
  T *rcdptr_;

  ReadElement(uint64_t key, T *rcdptr, const char *val)
      : key_(key), rcdptr_(rcdptr) {
    memcpy(this->val_, val, VAL_SIZE);
  }

  bool operator<(const ReadElement &right) const {
    return this->key_ < right.key_;
  }

  const char *get_val() const { return val_; }

private:
  char val_[VAL_SIZE];
};

template <typename T>
class WriteElement {
public:
  uint64_t key_;
  T *rcdptr_;

  WriteElement(uint64_t key, T *rcdptr, std::string_view val)
      : key_(key), rcdptr_(rcdptr) {
    if (val.size() != 0) {
      val_ptr_ = std::make_unique<char[]>(val.size());
      memcpy(val_ptr_.get(), val.data(), val.size());
      val_length_ = val.size();
    } else {
      val_length_ = 0;
    }
  }

  bool operator<(const WriteElement &right) const {
    return this->key_ < right.key_;
  }

  char *get_val_ptr() { return val_ptr_.get(); }

  std::size_t get_val_length() { return val_length_; }

private:
  std::unique_ptr<char[]> val_ptr_;
  std::size_t val_length_{};
};
