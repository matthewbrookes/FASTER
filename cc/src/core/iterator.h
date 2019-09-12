#include "persistent_memory_malloc.h"

namespace FASTER {
namespace core {

template <class K, class V, class D>
class FasterIterator;

template <class K, class V, class D>
class FasterIteratorRecord {
public:
    typedef K key_t;
    typedef V value_t;

    FasterIteratorRecord()
    : key_{ NULL }
    , value_ { NULL } {
    }

    inline constexpr const key_t* key() const {
      return key_;
    }

    inline constexpr const value_t* value() const {
      return value_;
    }

    friend class FasterIterator<K, V, D>;

private:
    const key_t* key_;
    const value_t* value_;
};

template<class K, class V, class D>
class FasterIterator {
public:
  typedef K key_t;
  typedef V value_t;

  FasterIterator(PersistentMemoryMalloc<D>* hlog, Address begin_address, Address end_address)
    : hlog_ { hlog }
    , current_address_ { 0 }
    , end_address_ { end_address }
    , next_address_{ begin_address }{
  }

  bool GetNext(FasterIteratorRecord<key_t, value_t, D>* out);

private:
  typedef Record<key_t, value_t> record_t;
  PersistentMemoryMalloc<D>* hlog_;
  Address current_address_;
  Address end_address_;
  Address next_address_;
};

template <class K, class V, class D>
bool FasterIterator<K, V, D>::GetNext(FasterIteratorRecord<K, V, D>* out) {
  current_address_ = next_address_;

  while (true) {
    if (current_address_ >= end_address_) {
      return false;
    }

    if (current_address_ < hlog_->begin_address.load()) {
      throw std::runtime_error("Iterator address is less than log beginAddress");
    }

    if (current_address_ < hlog_->head_address.load()) {
      throw std::runtime_error("Iterating over on-disk records unsupported");
    }

    auto record_ = reinterpret_cast<const record_t*>(hlog_->Get(current_address_));

    if (record_->header.invalid) {
      continue;
    }

    const key_t& key = record_->key();
    const value_t& value = record_->value();
    out->key_ = &key;
    out->value_ = &value;

    uint32_t record_size = record_->size();
    next_address_ += record_size;
    return true;
  }

}

}
}
