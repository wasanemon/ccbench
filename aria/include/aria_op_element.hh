#pragma once

#include <cstring>
#include <memory>

#include "../../include/op_element.hh"

template <typename T>
class ReadElement : public OpElement<T> {
public:
  using OpElement<T>::OpElement;

  ReadElement(uint64_t key, T *rcdptr, const char *val)
      : OpElement<T>::OpElement(key, rcdptr) {
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
class WriteElement : public OpElement<T> {
public:
  using OpElement<T>::OpElement;

  WriteElement(uint64_t key, T *rcdptr, std::string_view val)
      : OpElement<T>::OpElement(key, rcdptr) {
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
