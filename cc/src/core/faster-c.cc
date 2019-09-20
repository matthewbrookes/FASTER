// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.


#include "faster.h"
#include "faster-c.h"
#include "nexmark.h"
#include "device/file_system_disk.h"
#include "device/null_disk.h"

extern "C" {

  void deallocate_vec(uint8_t*, uint64_t);
  void deallocate_u64_vec(uint64_t*, uint64_t);
  void deallocate_string(char*);

  class Key {
    public:
      Key(const uint8_t* key, const uint64_t key_length)
        : temp_buffer_{ key }
        , key_length_{ key_length } {
      }

      Key(const Key& other) {
        key_length_ = other.key_length_;
        temp_buffer_ = NULL;
        if (other.temp_buffer_ == NULL) {
          memcpy(buffer(), other.buffer(), key_length_);
        } else {
          memcpy(buffer(), other.temp_buffer_, key_length_);
        }
      }

      ~Key() {
        if (this->temp_buffer_ != NULL) {
          deallocate_vec((uint8_t*) this->temp_buffer_, this->key_length_);
        }
      }

      /// Methods and operators required by the (implicit) interface:
      inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(Key) + key_length_);
      }
      inline KeyHash GetHash() const {
        if (this->temp_buffer_ != NULL) {
          return KeyHash(Utility::Hash8BitBytes(temp_buffer_, key_length_));
        }
        return KeyHash(Utility::Hash8BitBytes(buffer(), key_length_));
      }

      /// Comparison operators.
      inline bool operator==(const Key& other) const {
        if (this->key_length_ != other.key_length_) return false;
        const uint8_t* self_buffer = this->temp_buffer_ == NULL ? buffer() : this->temp_buffer_;
        const uint8_t* other_buffer = other.temp_buffer_ == NULL ? other.buffer() : other.temp_buffer_;
        return memcmp(self_buffer, other_buffer, key_length_) == 0;
      }
      inline bool operator!=(const Key& other) const {
        if (this->key_length_ != other.key_length_) return true;
        const uint8_t* self_buffer = this->temp_buffer_ == NULL ? buffer() : this->temp_buffer_;
        const uint8_t* other_buffer = other.temp_buffer_ == NULL ? other.buffer() : other.temp_buffer_;
        return memcmp(self_buffer, other_buffer, key_length_) != 0;
      }

      inline uint8_t* clone() const {
        uint8_t* clone = (uint8_t*) malloc(key_length_);
        memcpy(clone, buffer(), key_length_);
        return clone;
      }

      inline uint64_t length() const {
        return key_length_;
      }

    private:
      uint64_t key_length_;
      const uint8_t* temp_buffer_;

      inline const uint8_t* buffer() const {
        return reinterpret_cast<const uint8_t*>(this + 1);
      }
      inline uint8_t* buffer() {
        return reinterpret_cast<uint8_t*>(this + 1);
      }
  };

  class U64Key {
    public:
      U64Key(uint64_t key)
      : key_{ key } {
      }

      inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(U64Key));
      }
      inline KeyHash GetHash() const {
        return KeyHash{ Utility::GetHashCode(key_) };
      }

      inline uint64_t key() const {
        return key_;
      }

      /// Comparison operators.
      inline bool operator==(const U64Key& other) const {
        return key_ == other.key_;
      }
      inline bool operator!=(const U64Key& other) const {
        return key_ != other.key_;
      }

    private:
      uint64_t key_;
  };

  class ReadContext;
  class ReadPersonContext;
  class ReadU64Context;
  class UpsertContext;
  class UpsertPersonContext;
  class UpsertU64Context;
  class RmwContext;
  class RmwU64Context;
  class RmwDecreaseU64Context;

  class GenLock {
  public:
    GenLock()
      : control_{ 0 } {
    }
    GenLock(uint64_t control)
      : control_{ control } {
    }
    inline GenLock& operator=(const GenLock& other) {
      control_ = other.control_;
      return *this;
    }

    union {
      struct {
        uint64_t gen_number : 62;
        uint64_t locked : 1;
        uint64_t replaced : 1;
      };
      uint64_t control_;
    };
  };

  class AtomicGenLock {
  public:
    AtomicGenLock()
      : control_{ 0 } {
    }
    AtomicGenLock(uint64_t control)
      : control_{ control } {
    }

    inline GenLock load() const {
      return GenLock{ control_.load() };
    }
    inline void store(GenLock desired) {
      control_.store(desired.control_);
    }

    inline bool try_lock(bool& replaced) {
      replaced = false;
      GenLock expected{ control_.load() };
      expected.locked = 0;
      expected.replaced = 0;
      GenLock desired{ expected.control_ };
      desired.locked = 1;

      if(control_.compare_exchange_strong(expected.control_, desired.control_)) {
        return true;
      }
      if(expected.replaced) {
        replaced = true;
      }
      return false;
    }
    inline void unlock(bool replaced) {
      if(!replaced) {
        // Just turn off "locked" bit and increase gen number.
        uint64_t sub_delta = ((uint64_t)1 << 62) - 1;
        control_.fetch_sub(sub_delta);
      } else {
        // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
        uint64_t add_delta = ((uint64_t)1 << 63) - ((uint64_t)1 << 62) + 1;
        control_.fetch_add(add_delta);
      }
    }

  private:
    std::atomic<uint64_t> control_;
  };

  class Value {
  public:
    Value()
      : gen_lock_{ 0 }
      , size_{ 0 }
      , length_{ 0 } {
    }

    inline uint32_t size() const {
      return size_;
    }

    inline uint8_t* clone() const {
      uint8_t* clone = (uint8_t*) malloc(length_);
      memcpy(clone, buffer(), length_);
      return clone;
    }

    inline uint64_t length() const {
      return length_;
    }

    friend class ReadContext;
    friend class UpsertContext;
    friend class RmwContext;

  private:
    AtomicGenLock gen_lock_;
    uint64_t size_;
    uint64_t length_;

    inline const uint8_t* buffer() const {
      return reinterpret_cast<const uint8_t*>(this + 1);
    }
    inline uint8_t* buffer() {
      return reinterpret_cast<uint8_t*>(this + 1);
    }
  };

  class PersonValue {
  public:
      PersonValue()
      : name_length_{ 0 }
      , city_length_{ 0 }
      , state_length_ { 0 }{
      }

      inline uint32_t size() const {
        return sizeof(PersonValue) + name_length_ + city_length_ + state_length_;
      }

      const char* name() const {
        return reinterpret_cast<const char*>(this + 1);
      }

      const char* city() const {
        return reinterpret_cast<const char*>(name() + name_length_);
      }

      const char* state() const {
        return reinterpret_cast<const char*>(city() + city_length_);
      }

      char* name() {
        return reinterpret_cast<char*>(this + 1);
      }

      char* city() {
        return reinterpret_cast<char*>(name() + name_length_);
      }

      char* state() {
        return reinterpret_cast<char*>(city() + city_length_);
      }

      friend class ReadPersonContext;
      friend class UpsertPersonContext;
  private:
      size_t name_length_;
      size_t city_length_;
      size_t state_length_;
  };

class AuctionsValue {
public:
    AuctionsValue()
            : length_{ 0 } {
    }

    inline uint32_t size() const {
      return length_ * sizeof(uint64_t);
    }

    friend class ReadAuctionsContext;
    friend class UpsertAuctionsContext;
    friend class RmwAuctionContext;
    friend class RmwAuctionsContext;

private:
    uint64_t length_;

    inline const uint64_t* buffer() const {
      return reinterpret_cast<const uint64_t*>(this + 1);
    }
    inline uint64_t* buffer() {
      return reinterpret_cast<uint64_t*>(this + 1);
    }
};

class U64Value {
public:
    U64Value()
    : value_{ 0 } {

    }

    inline uint32_t size() const {
      return sizeof(U64Value);
    }

    inline uint64_t value() const {
      return value_;
    }

    friend class UpsertU64Context;
    friend class ReadU64Context;
    friend class RmwU64Context;
    friend class RmwDecreaseU64Context;
private:
    uint64_t value_;
};

class U64PairValue {
public:
    U64PairValue()
            : left_{ 0 }
            , right_{ 0 } {
    }

    inline uint32_t size() const {
      return sizeof(U64PairValue);
    }

    inline uint64_t left() const {
      return left_;
    }

    inline uint64_t right() const {
      return right_;
    }

    friend class UpsertU64PairContext;
    friend class ReadU64PairContext;
    friend class RmwU64PairContext;

private:
    uint64_t left_;
    uint64_t right_;
};

class TenElementsValue : public IAsyncContext {
public:
    TenElementsValue()
    : length_ { 0 }
    , tail_ { 0 } {
    }

    inline uint32_t size() const {
      return sizeof(TenElementsValue) + 10 * sizeof(size_t);
    }

    friend class RmwTenElementsContext;
    friend class ReadTenElementsContext;
private:
    uint8_t length_;
    uint8_t tail_;
    inline const size_t* buffer() const {
      return reinterpret_cast<const size_t*>(this + 1);
    }
    inline size_t* buffer() {
      return reinterpret_cast<size_t*>(this + 1);
    }
};

class AuctionBidsValue {
public:
    AuctionBidsValue()
            : bids_length_{ 0 } {
    }

    inline uint32_t size() const {
      return sizeof(AuctionBidsValue) + bids_length_ * sizeof(bid_t);
    }

    const bid_t* bids() const {
      return reinterpret_cast<const bid_t*>(this + 1);
    }

    bid_t* bids() {
      return reinterpret_cast<bid_t*>(this + 1);
    }

    friend class ReadAuctionBidsContext;
    friend class RmwAuctionBidsAuctionContext;
    friend class RmwAuctionBidsBidContext;
private:
    auction_t auction;
    size_t bids_length_;
};

  class ReadContext : public IAsyncContext {
  public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(const uint8_t* key, uint64_t key_length, read_callback cb, void* target)
      : key_{ key, key_length }
      , cb_ { cb }
      , target_ { target }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
      : key_{ other.key_ }
      , cb_ { other.cb_ }
      , target_ { other.target_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }

    inline void Get(const Value& value) {
      cb_(target_, value.buffer(), value.length_, Ok);
    }
    inline void GetAtomic(const Value& value) {
      GenLock before, after;
      uint8_t* buffer = NULL;
      uint64_t length = 0;
      do {
        before = value.gen_lock_.load();
        buffer = (uint8_t*) realloc(buffer, value.length_);
        memcpy(buffer, value.buffer(), value.length_);
        length = value.length_;
        after = value.gen_lock_.load();
      } while(before.gen_number != after.gen_number);
      cb_(target_, buffer, length, Ok);
      free(buffer);
    }

    /// For async reads returning not found
    inline void ReturnNotFound() {
      cb_(target_, NULL, 0, NotFound);
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    Key key_;
    read_callback cb_;
    void* target_;
  };

class ReadPersonContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef PersonValue value_t;

    ReadPersonContext(const key_t& key, read_person_callback cb, void* target)
            : key_{ key }
            , cb_ { cb }
            , target_ { target }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadPersonContext(const ReadPersonContext& other)
            : key_{ other.key_ }
            , cb_ { other.cb_ }
            , target_ { other.target_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      person_t person;
      person.name = value.name();
      person.city = value.city();
      person.state = value.state();
      cb_(target_, person, Ok);
    }
    inline void GetAtomic(const value_t& value) {
      person_t person;
      person.name = value.name();
      person.city = value.city();
      person.state = value.state();
      cb_(target_, person, Ok);
    }

    /// For async reads returning not found
    inline void ReturnNotFound() {
      cb_(target_, person_t {}, NotFound);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    read_person_callback cb_;
    void* target_;
};

class ReadAuctionsContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef AuctionsValue value_t;

    ReadAuctionsContext(const key_t& key, read_auctions_callback cb, void* target)
            : key_{ key }
            , cb_ { cb }
            , target_ { target }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadAuctionsContext(const ReadAuctionsContext& other)
            : key_{ other.key_ }
            , cb_ { other.cb_ }
            , target_ { other.target_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      cb_(target_, value.buffer(), value.length_, Ok);
    }
    inline void GetAtomic(const value_t& value) {
      cb_(target_, value.buffer(), value.length_, Ok);
    }

    /// For async reads returning not found
    inline void ReturnNotFound() {
      cb_(target_, NULL, 0, NotFound);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    read_auctions_callback cb_;
    void* target_;
};

class ReadU64Context : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef U64Value value_t;

    ReadU64Context(const key_t& key, read_u64_callback cb, void* target)
            : key_{ key }
            , cb_ { cb }
            , target_ { target }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadU64Context(const ReadU64Context& other)
            : key_{ other.key_ }
            , cb_ { other.cb_ }
            , target_ { other.target_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      cb_(target_, value.value_, Ok);
    }
    inline void GetAtomic(const value_t& value) {
      cb_(target_, value.value_, Ok);
    }

    /// For async reads returning not found
    inline void ReturnNotFound() {
      cb_(target_, 0, NotFound);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    read_u64_callback cb_;
    void* target_;
};

class ReadU64PairContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef U64PairValue value_t;

    ReadU64PairContext(const key_t& key, read_u64_pair_callback cb, void* target)
            : key_{ key }
            , cb_ { cb }
            , target_ { target }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadU64PairContext(const ReadU64PairContext& other)
            : key_{ other.key_ }
            , cb_ { other.cb_ }
            , target_ { other.target_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      cb_(target_, (uint64_t*)&value.left_, (uint64_t*)&value.right_, Ok);
    }
    inline void GetAtomic(const value_t& value) {
      cb_(target_, (uint64_t*)&value.left_, (uint64_t*)&value.right_, Ok);
    }

    /// For async reads returning not found
    inline void ReturnNotFound() {
      cb_(target_, 0, 0, NotFound);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    read_u64_pair_callback cb_;
    void* target_;
};

class ReadTenElementsContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef TenElementsValue value_t;

    ReadTenElementsContext(const key_t& key, read_ten_elements_callback cb, void* target)
            : key_{ key }
            , cb_ { cb }
            , target_ { target }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadTenElementsContext(const ReadTenElementsContext& other)
            : key_{ other.key_ }
            , cb_ { other.cb_ }
            , target_ { other.target_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      size_t sum = 0;
      for (size_t i = 0; i < value.length_; ++i) {
        sum += value.buffer()[i];
      }
      cb_(target_, sum / value.length_, Ok);
    }
    inline void GetAtomic(const value_t& value) {
      size_t sum = 0;
      for (size_t i = 0; i < value.length_; ++i) {
        sum += value.buffer()[i];
      }
      cb_(target_, sum / value.length_, Ok);
    }

    /// For async reads returning not found
    inline void ReturnNotFound() {
      cb_(target_, 0, NotFound);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    read_ten_elements_callback cb_;
    void* target_;
};

class ReadAuctionBidsContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef AuctionBidsValue value_t;

    ReadAuctionBidsContext(const key_t& key, read_auction_bids_callback cb, void* target)
            : key_{ key }
            , cb_ { cb }
            , target_ { target }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadAuctionBidsContext(const ReadAuctionBidsContext& other)
            : key_{ other.key_ }
            , cb_ { other.cb_ }
            , target_ { other.target_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      cb_(target_, &value.auction, value.bids(), value.bids_length_, Ok);
    }
    inline void GetAtomic(const value_t& value) {
      cb_(target_, &value.auction, value.bids(), value.bids_length_, Ok);
    }

    /// For async reads returning not found
    inline void ReturnNotFound() {
      cb_(target_, NULL, NULL, 0, NotFound);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    read_auction_bids_callback cb_;
    void* target_;
};


  class UpsertContext : public IAsyncContext {
  public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(const uint8_t* key, uint64_t key_length, uint8_t* input, uint64_t length)
      : key_{ key, key_length }
      , input_{ input }
      , length_{ length } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(UpsertContext& other)
      : key_{ other.key_ }
      , input_{ other.input_ }
      , length_{ other.length_ } {
      other.input_ = NULL;
    }

    ~UpsertContext() {
      if (input_ != NULL) {
        deallocate_vec(input_, length_);
      }
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(Value) + length_;
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(Value& value) {
      value.gen_lock_.store(0);
      value.size_ = sizeof(Value) + length_;
      value.length_ = length_;
      std::memcpy(value.buffer(), input_, length_);
    }
    inline bool PutAtomic(Value& value) {
      bool replaced;
      while(!value.gen_lock_.try_lock(replaced) && !replaced) {
        std::this_thread::yield();
      }
      if(replaced) {
        // Some other thread replaced this record.
        return false;
      }
      if(value.size_ < sizeof(Value) + length_) {
        // Current value is too small for in-place update.
        value.gen_lock_.unlock(true);
        return false;
      }
      // In-place update overwrites length and buffer, but not size.
      value.length_ = length_;
      std::memcpy(value.buffer(), input_, length_);
      value.gen_lock_.unlock(false);
      return true;
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    key_t key_;
    uint8_t* input_;
    uint64_t length_;
  };

class UpsertPersonContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef PersonValue value_t;

    UpsertPersonContext(const key_t& key, const person_t input)
            : key_{ key }
            , input_{ input } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertPersonContext(UpsertPersonContext& other)
            : key_{ other.key_ }
            , input_{ other.input_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + input_.name_length + input_.city_length + input_.state_length;
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t& value) {
      value.name_length_ = input_.name_length;
      value.city_length_ = input_.city_length;
      value.state_length_ = input_.state_length;
      memcpy(value.name(), input_.name, input_.name_length);
      memcpy(value.city(), input_.city, input_.city_length);
      memcpy(value.state(), input_.state, input_.state_length);
      deallocate_string((char *) input_.name);
      deallocate_string((char *) input_.city);
      deallocate_string((char *) input_.state);
    }
    inline bool PutAtomic(value_t& value) {
      value.name_length_ = input_.name_length;
      value.city_length_ = input_.city_length;
      value.state_length_ = input_.state_length;
      memcpy(value.name(), input_.name, input_.name_length);
      memcpy(value.city(), input_.city, input_.city_length);
      memcpy(value.state(), input_.state, input_.state_length);
      deallocate_string((char *) input_.name);
      deallocate_string((char *) input_.city);
      deallocate_string((char *) input_.state);
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    person_t input_;
};

class UpsertAuctionsContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef AuctionsValue value_t;

    UpsertAuctionsContext(const key_t& key, uint64_t* input, uint64_t length)
            : key_{ key }
            , input_{ input }
            , length_{ length } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertAuctionsContext(UpsertAuctionsContext& other)
            : key_{ other.key_ }
            , input_{ other.input_ }
            , length_{ other.length_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + length_ * sizeof(uint64_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t& value) {
      value.length_ = length_;
      memcpy(value.buffer(), input_, length_ * sizeof(uint64_t));
      deallocate_u64_vec(input_, length_);
    }
    inline bool PutAtomic(value_t& value) {
      if (value.length_ < length_) {
        return false;
      }
      value.length_ = length_;
      memcpy(value.buffer(), input_, length_ * sizeof(uint64_t));
      deallocate_u64_vec(input_, length_);
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    uint64_t* input_;
    uint64_t length_;
};

class UpsertU64Context : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef U64Value value_t;

    UpsertU64Context(const key_t& key, const uint64_t input)
            : key_{ key }
            , input_{ input } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertU64Context(UpsertU64Context& other)
            : key_{ other.key_ }
            , input_{ other.input_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t& value) {
      value.value_ = input_;
    }
    inline bool PutAtomic(value_t& value) {
      value.value_ = input_;
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    uint64_t input_;
};

class UpsertU64PairContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef U64PairValue value_t;

    UpsertU64PairContext(const key_t& key, const uint64_t left, const uint64_t right)
            : key_{ key }
            , left_{ left }
            , right_ { right }{
    }

    /// Copy (and deep-copy) constructor.
    UpsertU64PairContext(UpsertU64PairContext& other)
            : key_{ other.key_ }
            , left_{ other.left_ }
            , right_ { other.right_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t);
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t& value) {
      value.left_ = left_;
      value.right_ = right_;
    }
    inline bool PutAtomic(value_t& value) {
      value.left_ = left_;
      value.right_ = right_;
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    uint64_t left_;
    uint64_t right_;
};

  class RmwContext : public IAsyncContext {
  public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(const uint8_t* key, uint64_t key_length, uint8_t* modification, uint64_t length, rmw_callback cb)
      : key_{ key, key_length }
      , modification_{ modification }
      , length_{ length }
      , cb_{ cb }
      , new_length_{ 0 }{
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(RmwContext& other)
      : key_{ other.key_ }
      , modification_{ other.modification_ }
      , length_{ other.length_ }
      , cb_{ other.cb_ }
      , new_length_{ other.new_length_ }{
      other.modification_ = NULL;
    }

    ~RmwContext() {
      if (modification_ != NULL) {
        deallocate_vec(modification_, length_);
      }
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(Value) + length_;
    }
    inline uint32_t value_size(const Value& old_value) {
      if (new_length_ == 0) {
        new_length_ = cb_(old_value.buffer(), old_value.length_, modification_, length_, NULL);
      }
      return sizeof(Value) + new_length_;
    }

    inline void RmwInitial(Value& value) {
      value.gen_lock_.store(0);
      value.size_ = sizeof(Value) + length_;
      value.length_ = length_;
      std::memcpy(value.buffer(), modification_, length_);
    }
    inline void RmwCopy(const Value& old_value, Value& value) {
      value.gen_lock_.store(0);
      value.length_ = cb_(old_value.buffer(), old_value.length_, modification_, length_, value.buffer());
      value.size_ = sizeof(Value) + value.length_;
    }
    inline bool RmwAtomic(Value& value) {
      bool replaced;
      while(!value.gen_lock_.try_lock(replaced) && !replaced) {
        std::this_thread::yield();
      }
      if(replaced) {
        // Some other thread replaced this record.
        return false;
      }
      if (new_length_ == 0) {
        new_length_ = cb_(value.buffer(), value.length_, modification_, length_, NULL);
      }
      if(value.size_ < sizeof(Value) + new_length_) {
        // Current value is too small for in-place update.
        value.gen_lock_.unlock(true);
        return false;
      }
      // In-place update overwrites length and buffer, but not size.
      cb_(value.buffer(), value.length_, modification_, length_, value.buffer());
      value.length_ = new_length_;
      value.gen_lock_.unlock(false);
      return true;
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    Key key_;
    uint8_t* modification_;
    uint64_t length_;
    rmw_callback cb_;
    uint64_t new_length_;
  };

class RmwAuctionContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef AuctionsValue value_t;

    RmwAuctionContext(const uint64_t key, const uint64_t modification)
            : key_{ key }
            , modification_{ modification } {
    }

    /// Copy (and deep-copy) constructor.
    RmwAuctionContext(RmwAuctionContext& other)
            : key_{ other.key_ }
            , modification_{ other.modification_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + sizeof(uint64_t);
    }
    inline uint32_t value_size(const value_t& old_value) {
      return sizeof(value_t) + (old_value.length_ + 1) * sizeof(uint64_t);
    }

    inline void RmwInitial(value_t& value) {
      value.length_ = 1;
      std::memcpy(value.buffer(), &modification_, sizeof(uint64_t));
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.length_ = old_value.length_ + 1;
      std::memcpy(value.buffer(), old_value.buffer(), old_value.length_ * sizeof(uint64_t));
      std::memcpy(value.buffer() + old_value.length_, &modification_, sizeof(uint64_t));
    }
    inline bool RmwAtomic(value_t& value) {
      // Value always grows so no in-place possible
      return false;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    const uint64_t modification_;
};

class RmwAuctionsContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef AuctionsValue value_t;

    RmwAuctionsContext(const uint64_t key, uint64_t* modification, uint64_t length)
            : key_{ key }
            , modification_{ modification }
            , length_{ length }{
    }

    /// Copy (and deep-copy) constructor.
    RmwAuctionsContext(RmwAuctionsContext& other)
            : key_{ other.key_ }
            , modification_{ other.modification_ }
            , length_{ other.length_ }{
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + sizeof(uint64_t);
    }
    inline uint32_t value_size(const value_t& old_value) {
      return sizeof(value_t) + (old_value.length_ + length_) * sizeof(uint64_t);
    }

    inline void RmwInitial(value_t& value) {
      value.length_ = length_;
      std::memcpy(value.buffer(), &modification_, length_ * sizeof(uint64_t));
      deallocate_u64_vec(modification_, length_);
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.length_ = old_value.length_ + length_;
      std::memcpy(value.buffer(), old_value.buffer(), old_value.length_ * sizeof(uint64_t));
      std::memcpy(value.buffer() + old_value.length_, modification_, length_ * sizeof(uint64_t));
      deallocate_u64_vec(modification_, length_);
    }
    inline bool RmwAtomic(value_t& value) {
      // Value always grows so no in-place possible
      return false;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    uint64_t* modification_;
    uint64_t length_;
};

class RmwU64Context : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef U64Value value_t;

    RmwU64Context(const uint64_t key, uint64_t modification)
            : key_{ key }
            , modification_{ modification } {
    }

    /// Copy (and deep-copy) constructor.
    RmwU64Context(RmwU64Context& other)
            : key_{ other.key_ }
            , modification_{ other.modification_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t);
    }
    inline uint32_t value_size(const value_t& old_value) const {
      return sizeof(value_t);
    }

    inline void RmwInitial(value_t& value) {
      value.value_ = modification_;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.value_ = old_value.value_ + modification_;
    }
    inline bool RmwAtomic(value_t& value) {
      value.value_ += modification_;
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    uint64_t modification_;
};

class RmwDecreaseU64Context : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef U64Value value_t;

    RmwDecreaseU64Context(const uint64_t key, uint64_t modification)
            : key_{ key }
            , modification_{ modification } {
    }

    /// Copy (and deep-copy) constructor.
    RmwDecreaseU64Context(RmwDecreaseU64Context& other)
            : key_{ other.key_ }
            , modification_{ other.modification_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t);
    }
    inline uint32_t value_size(const value_t& old_value) const {
      return sizeof(value_t);
    }

    inline void RmwInitial(value_t& value) {
      value.value_ = -modification_;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.value_ = old_value.value_ - modification_;
    }
    inline bool RmwAtomic(value_t& value) {
      value.value_ -= modification_;
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    uint64_t modification_;
};

class RmwU64PairContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef U64PairValue value_t;

    RmwU64PairContext(const uint64_t key, uint64_t left, uint64_t right)
            : key_{ key }
            , left_{ left }
            , right_{ right } {
    }

    /// Copy (and deep-copy) constructor.
    RmwU64PairContext(RmwU64PairContext& other)
            : key_{ other.key_ }
            , left_{ other.left_ }
            , right_{  other.right_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t);
    }
    inline uint32_t value_size(const value_t& old_value) const {
      return sizeof(value_t);
    }

    inline void RmwInitial(value_t& value) {
      value.left_ = left_;
      value.right_ = right_;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.left_ = old_value.left_ + left_;
      value.right_ = old_value.right_ + right_;
    }
    inline bool RmwAtomic(value_t& value) {
      value.left_ += left_;
      value.right_ += right_;
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    uint64_t left_;
    uint64_t right_;
};

class RmwTenElementsContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef TenElementsValue value_t;

    RmwTenElementsContext(const uint64_t key, size_t modification)
            : key_{ key }
            , modification_{ modification } {
    }

    /// Copy (and deep-copy) constructor.
    RmwTenElementsContext(RmwTenElementsContext& other)
            : key_{ other.key_ }
            , modification_{ other.modification_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + 10 * sizeof(size_t);
    }
    inline uint32_t value_size(const value_t& old_value) const {
      return old_value.size();
    }

    inline void RmwInitial(value_t& value) {
      for (size_t i = 1; i < 10; ++i) {
        value.buffer()[i] = 0;
      }
      value.length_ = 1;
      value.buffer()[value.tail_] = modification_;
      value.tail_ = (value.tail_ + 1) % 10;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.length_ = old_value.length_ < 10 ? old_value.length_ + 1 : 10;
      memcpy(value.buffer(), old_value.buffer(), 10 * sizeof(size_t));
      value.tail_ = old_value.tail_;
      value.buffer()[value.tail_] = modification_;
      value.tail_ = (value.tail_ + 1) % 10;
    }
    inline bool RmwAtomic(value_t& value) {
      value.length_ = value.length_ < 10 ? value.length_ + 1 : 10;
      value.buffer()[value.tail_] = modification_;
      value.tail_ = (value.tail_ + 1) % 10;
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    size_t modification_;
};

class RmwAuctionBidsAuctionContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef AuctionBidsValue value_t;

    RmwAuctionBidsAuctionContext(const uint64_t key, const auction_t modification)
            : key_{ key }
            , modification_{ modification } {
    }

    /// Copy (and deep-copy) constructor.
    RmwAuctionBidsAuctionContext(RmwAuctionBidsAuctionContext& other)
            : key_{ other.key_ }
            , modification_{ other.modification_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t);
    }
    inline uint32_t value_size(const value_t& old_value) {
      return sizeof(value_t) + old_value.bids_length_ * sizeof(bid_t);
    }

    inline void RmwInitial(value_t& value) {
      value.auction = modification_;
      value.bids_length_ = 0;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.auction = modification_;
      value.bids_length_ = old_value.bids_length_;
      std::memcpy(value.bids(), old_value.bids(), old_value.bids_length_ * sizeof(bid_t));
    }
    inline bool RmwAtomic(value_t& value) {
      value.auction = modification_;
      return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    auction_t modification_;
};

class RmwAuctionBidsBidContext : public IAsyncContext {
public:
    typedef U64Key key_t;
    typedef AuctionBidsValue value_t;

    RmwAuctionBidsBidContext(const uint64_t key, const bid_t modification)
            : key_{ key }
            , modification_{ modification } {
    }

    /// Copy (and deep-copy) constructor.
    RmwAuctionBidsBidContext(RmwAuctionBidsBidContext& other)
            : key_{ other.key_ }
            , modification_{ other.modification_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + sizeof(bid_t);
    }
    inline uint32_t value_size(const value_t& old_value) {
      return sizeof(value_t) + (old_value.bids_length_ + 1) * sizeof(bid_t);
    }

    inline void RmwInitial(value_t& value) {
      value.bids()[0] = modification_;
      value.bids_length_ = 1;
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      value.auction = old_value.auction;
      value.bids_length_ = old_value.bids_length_ + 1;
      std::memcpy(value.bids(), old_value.bids(), old_value.bids_length_ * sizeof(bid_t));
      value.bids()[old_value.bids_length_] = modification_;
    }
    inline bool RmwAtomic(value_t& value) {
      return false;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    key_t key_;
    bid_t modification_;
};

  class DeleteContext: public IAsyncContext {
  public:
      typedef Key key_t;
      typedef Value value_t;

      DeleteContext(const uint8_t* key, const uint64_t key_length)
      : key_ { key, key_length } {
      }

      DeleteContext(const DeleteContext& other)
      : key_ { other.key_ } {
      }

      inline const Key& key() const {
        return key_;
      }

  protected:
      /// The explicit interface requires a DeepCopy_Internal() implementation.
      Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }

  private:
      const key_t key_;
  };

  class DeleteU64Context: public IAsyncContext {
  public:
      typedef U64Key key_t;
      typedef U64Value value_t;

      DeleteU64Context(const uint64_t key)
              : key_ { key } {
      }

      DeleteU64Context(const DeleteU64Context& other)
              : key_ { other.key_ } {
      }

      inline const U64Key& key() const {
        return key_;
      }

  protected:
      /// The explicit interface requires a DeepCopy_Internal() implementation.
      Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }

  private:
      const key_t key_;
  };

  enum store_type {
      NULL_DISK,
      FILESYSTEM_DISK,
      PERSON_STORE,
      AUCTIONS_STORE,
      U64_STORE,
      U64_PAIR_STORE,
      TEN_ELEMENTS_STORE,
      AUCTION_BIDS_STORE,
  };
  typedef enum store_type store_type;

  typedef FASTER::environment::QueueIoHandler handler_t;
  typedef FASTER::device::FileSystemDisk<handler_t, 1073741824L> disk_t;
  typedef FASTER::device::NullDisk  disk_null_t;
  using store_t = FasterKv<Key, Value, disk_t>;
  using null_store_t = FasterKv<Key, Value, disk_null_t>;
  using store_people_t = FasterKv<U64Key, PersonValue, disk_t>;
  using store_auctions_t = FasterKv<U64Key, AuctionsValue, disk_t>;
  using store_u64_t = FasterKv<U64Key, U64Value, disk_t>;
  using store_u64_pair_t = FasterKv<U64Key, U64PairValue, disk_t>;
  using store_ten_elements_t = FasterKv<U64Key, TenElementsValue, disk_t>;
  using store_auction_bids_t = FasterKv<U64Key, AuctionBidsValue, disk_t>;
  struct faster_t {
      union {
          store_t* store;
          null_store_t* null_store;
          store_people_t* people_store;
          store_auctions_t* auctions_store;
          store_u64_t* u64_store;
          store_u64_pair_t* u64_pair_store;
          store_ten_elements_t* ten_elements_store;
          store_auction_bids_t * auction_bids_store;
      } obj;
      store_type type;
  };

  faster_t* faster_open(const uint64_t table_size, const uint64_t log_size) {
    faster_t* res = new faster_t();
    res->obj.null_store = new null_store_t { table_size, log_size, "" };
    res->type = NULL_DISK;
    return res;
  }

  faster_t* faster_open_with_disk(const uint64_t table_size, const uint64_t log_size, const char* storage) {
    faster_t* res = new faster_t();
    std::experimental::filesystem::create_directory(storage);
    res->obj.store= new store_t { table_size, log_size, storage };
    res->type = FILESYSTEM_DISK;
    return res;
  }

  faster_t* faster_open_with_disk_people(const uint64_t table_size, const uint64_t log_size, const char* storage) {
    faster_t* res = new faster_t();
    std::experimental::filesystem::create_directory(storage);
    res->obj.people_store= new store_people_t { table_size, log_size, storage };
    res->type = PERSON_STORE;
    return res;
  }

  faster_t* faster_open_with_disk_auctions(const uint64_t table_size, const uint64_t log_size, const char* storage) {
    faster_t* res = new faster_t();
    std::experimental::filesystem::create_directory(storage);
    res->obj.auctions_store= new store_auctions_t { table_size, log_size, storage };
    res->type = AUCTIONS_STORE;
    return res;
  }

  faster_t* faster_open_with_disk_u64(const uint64_t table_size, const uint64_t log_size, const char* storage) {
    faster_t* res = new faster_t();
    std::experimental::filesystem::create_directory(storage);
    res->obj.u64_store= new store_u64_t { table_size, log_size, storage };
    res->type = U64_STORE;
    return res;
  }

faster_t* faster_open_with_disk_u64_pair(const uint64_t table_size, const uint64_t log_size, const char* storage) {
  faster_t* res = new faster_t();
  std::experimental::filesystem::create_directory(storage);
  res->obj.u64_pair_store= new store_u64_pair_t { table_size, log_size, storage };
  res->type = U64_PAIR_STORE;
  return res;
}

faster_t* faster_open_with_disk_ten_elements(const uint64_t table_size, const uint64_t log_size, const char* storage) {
  faster_t* res = new faster_t();
  std::experimental::filesystem::create_directory(storage);
  res->obj.ten_elements_store = new store_ten_elements_t { table_size, log_size, storage };
  res->type = TEN_ELEMENTS_STORE;
  return res;
}

faster_t* faster_open_with_disk_auction_bids(const uint64_t table_size, const uint64_t log_size, const char* storage) {
  faster_t* res = new faster_t();
  std::experimental::filesystem::create_directory(storage);
  res->obj.auction_bids_store = new store_auction_bids_t { table_size, log_size, storage };
  res->type = AUCTION_BIDS_STORE;
  return res;
}

  uint8_t faster_upsert(faster_t* faster_t, const uint8_t* key, const uint64_t key_length,
                        uint8_t* value, uint64_t value_length, const uint64_t monotonic_serial_number) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
    };

    UpsertContext context { key, key_length, value, value_length };
    Status result;
    switch (faster_t->type) {
      case NULL_DISK:
        result = faster_t->obj.null_store->Upsert(context, callback, monotonic_serial_number);
        break;
      case FILESYSTEM_DISK:
        result = faster_t->obj.store->Upsert(context, callback, monotonic_serial_number);
        break;
    }
    return static_cast<uint8_t>(result);
  }

uint8_t faster_upsert_person(faster_t* faster_t, const uint64_t key, person_t person, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
  };

  UpsertPersonContext context { key, person };
  Status result = faster_t->obj.people_store->Upsert(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_upsert_auctions(faster_t* faster_t, const uint64_t key, uint64_t* input, uint64_t length, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
  };

  UpsertAuctionsContext context { key, input, length };
  Status result = faster_t->obj.auctions_store->Upsert(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_upsert_u64(faster_t* faster_t, const uint64_t key, const uint64_t input, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
  };

  UpsertU64Context context { key, input };
  Status result = faster_t->obj.u64_store->Upsert(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_upsert_u64_pair(faster_t* faster_t, const uint64_t key, const uint64_t left, const uint64_t right, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      assert(result == Status::Ok);
  };

  UpsertU64PairContext context { key, left, right };
  Status result = faster_t->obj.u64_pair_store->Upsert(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

  uint8_t faster_rmw(faster_t* faster_t, const uint8_t* key, const uint64_t key_length, uint8_t* modification,
                     const uint64_t length, const uint64_t monotonic_serial_number, rmw_callback cb) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwContext> context { ctxt };
    };

    RmwContext context{ key, key_length, modification, length, cb};
    Status result;
    switch (faster_t->type) {
      case NULL_DISK:
        result = faster_t->obj.null_store->Rmw(context, callback, monotonic_serial_number);
        break;
      case FILESYSTEM_DISK:
        result = faster_t->obj.store->Rmw(context, callback, monotonic_serial_number);
        break;
    }
    return static_cast<uint8_t>(result);
  }

  uint8_t faster_rmw_auction(faster_t* faster_t, const uint64_t key, const uint64_t modification, const uint64_t monotonic_serial_number) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwAuctionContext> context { ctxt };
    };

    RmwAuctionContext context{ key, modification };
    Status result = faster_t->obj.auctions_store->Rmw(context, callback, monotonic_serial_number);
    return static_cast<uint8_t>(result);
  }

  uint8_t faster_rmw_auctions(faster_t* faster_t, const uint64_t key, uint64_t* modification, uint64_t length, const uint64_t monotonic_serial_number) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwAuctionsContext> context { ctxt };
    };

    RmwAuctionsContext context{ key, modification, length };
    Status result = faster_t->obj.auctions_store->Rmw(context, callback, monotonic_serial_number);
    return static_cast<uint8_t>(result);
  }

uint8_t faster_rmw_u64(faster_t* faster_t, const uint64_t key, uint64_t modification, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwU64Context> context { ctxt };
  };

  RmwU64Context context{ key, modification };
  Status result = faster_t->obj.u64_store->Rmw(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_rmw_decrease_u64(faster_t* faster_t, const uint64_t key, uint64_t modification, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwU64Context> context { ctxt };
  };

  RmwDecreaseU64Context context{ key, modification };
  Status result = faster_t->obj.u64_store->Rmw(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_rmw_u64_pair(faster_t* faster_t, const uint64_t key, uint64_t left, uint64_t right, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwU64PairContext> context { ctxt };
  };

  RmwU64PairContext context{ key, left, right };
  Status result = faster_t->obj.u64_pair_store->Rmw(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_rmw_ten_elements(faster_t* faster_t, const uint64_t key, size_t modification, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwTenElementsContext> context { ctxt };
  };

  RmwTenElementsContext context{ key, modification };
  Status result = faster_t->obj.ten_elements_store->Rmw(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_rmw_auction_bids_auction(faster_t* faster_t, const uint64_t key, auction_t modification, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwAuctionBidsAuctionContext> context { ctxt };
  };

  RmwAuctionBidsAuctionContext context{ key, modification };
  Status result = faster_t->obj.auction_bids_store->Rmw(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_rmw_auction_bids_bid(faster_t* faster_t, const uint64_t key, bid_t modification, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RmwAuctionBidsBidContext> context { ctxt };
  };

  RmwAuctionBidsBidContext context{ key, modification };
  Status result = faster_t->obj.auction_bids_store->Rmw(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

  uint8_t faster_read(faster_t* faster_t, const uint8_t* key, const uint64_t key_length,
                       const uint64_t monotonic_serial_number, read_callback cb, void* target) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadContext> context { ctxt };
      if (result == Status::NotFound) {
        context->ReturnNotFound();
      }
    };

    ReadContext context {key, key_length, cb, target};
    Status result;
    switch (faster_t->type) {
      case NULL_DISK:
        result = faster_t->obj.null_store->Read(context, callback, monotonic_serial_number);
        break;
      case FILESYSTEM_DISK:
        result = faster_t->obj.store->Read(context, callback, monotonic_serial_number);
        break;
    }

    if (result == Status::NotFound) {
      cb(target, NULL, 0, NotFound);
    }

    return static_cast<uint8_t>(result);
  }


uint8_t faster_read_person(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_person_callback cb, void* target) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadPersonContext> context { ctxt };
      if (result == Status::NotFound) {
        context->ReturnNotFound();
      }
  };

  ReadPersonContext context {key, cb, target};
  Status result = faster_t->obj.people_store->Read(context, callback, monotonic_serial_number);

  if (result == Status::NotFound) {
    cb(target, person_t {}, NotFound);
  }

  return static_cast<uint8_t>(result);
}

uint8_t faster_read_auctions(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_auctions_callback cb, void* target) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadAuctionsContext> context { ctxt };
      if (result == Status::NotFound) {
        context->ReturnNotFound();
      }
  };

  ReadAuctionsContext context {key, cb, target};
  Status result = faster_t->obj.auctions_store->Read(context, callback, monotonic_serial_number);

  if (result == Status::NotFound) {
    cb(target, NULL, 0, NotFound);
  }

  return static_cast<uint8_t>(result);
}

uint8_t faster_read_u64(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_u64_callback cb, void* target) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadU64Context> context { ctxt };
      if (result == Status::NotFound) {
        context->ReturnNotFound();
      }
  };

  ReadU64Context context {key, cb, target};
  Status result = faster_t->obj.u64_store->Read(context, callback, monotonic_serial_number);

  if (result == Status::NotFound) {
    cb(target, 0, NotFound);
  }

  return static_cast<uint8_t>(result);
}

uint8_t faster_read_u64_pair(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_u64_pair_callback cb, void* target) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadU64PairContext> context { ctxt };
      if (result == Status::NotFound) {
        context->ReturnNotFound();
      }
  };

  ReadU64PairContext context {key, cb, target};
  Status result = faster_t->obj.u64_pair_store->Read(context, callback, monotonic_serial_number);

  if (result == Status::NotFound) {
    cb(target, NULL, NULL, NotFound);
  }

  return static_cast<uint8_t>(result);
}

uint8_t faster_read_ten_elements(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_ten_elements_callback cb, void* target) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadTenElementsContext> context { ctxt };
      if (result == Status::NotFound) {
        context->ReturnNotFound();
      }
  };

  ReadTenElementsContext context {key, cb, target};
  Status result = faster_t->obj.ten_elements_store->Read(context, callback, monotonic_serial_number);

  if (result == Status::NotFound) {
    cb(target, 0, NotFound);
  }

  return static_cast<uint8_t>(result);
}

uint8_t faster_read_auction_bids(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_auction_bids_callback cb, void* target) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<ReadAuctionBidsContext> context { ctxt };
      if (result == Status::NotFound) {
        context->ReturnNotFound();
      }
  };

  ReadAuctionBidsContext context {key, cb, target};
  Status result = faster_t->obj.auction_bids_store->Read(context, callback, monotonic_serial_number);

  if (result == Status::NotFound) {
    cb(target, NULL, NULL, 0, NotFound);
  }

  return static_cast<uint8_t>(result);
}

uint8_t faster_delete(faster_t* faster_t, const uint8_t* key, const uint64_t key_length, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<DeleteContext> context { ctxt };
      assert(result == Status::Ok);
  };

  DeleteContext context { key, key_length };
  Status result;
  switch (faster_t->type) {
    case NULL_DISK:
      result = faster_t->obj.null_store->Delete(context, callback, monotonic_serial_number);
      break;
    case FILESYSTEM_DISK:
      result = faster_t->obj.store->Delete(context, callback, monotonic_serial_number);
      break;
  }
  return static_cast<uint8_t>(result);
}

uint8_t faster_delete_u64(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<DeleteU64Context> context { ctxt };
      assert(result == Status::Ok);
  };

  DeleteU64Context context { key };
  Status result = faster_t->obj.u64_store->Delete(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_delete_auctions(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<DeleteU64Context> context { ctxt };
      assert(result == Status::Ok);
  };

  DeleteU64Context context { key };
  Status result = faster_t->obj.auctions_store->Delete(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

uint8_t faster_delete_auction_bids(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<DeleteU64Context> context { ctxt };
      assert(result == Status::Ok);
  };

  DeleteU64Context context { key };
  Status result = faster_t->obj.auction_bids_store->Delete(context, callback, monotonic_serial_number);
  return static_cast<uint8_t>(result);
}

void* faster_scan_in_memory_init(faster_t* faster_t) {
  FasterIterator<Key, Value, disk_t>* iterator;
  switch (faster_t->type) {
    case NULL_DISK:
      break;
    case FILESYSTEM_DISK:
      iterator = faster_t->obj.store->ScanInMemory();
      break;
  }
  return iterator;
}

void* faster_scan_in_memory_init_u64(faster_t* faster_t) {
  return faster_t->obj.u64_store->ScanInMemory();
}

void* faster_scan_in_memory_init_u64_pair(faster_t* faster_t) {
  return faster_t->obj.u64_pair_store->ScanInMemory();
}

void faster_scan_in_memory_destroy(void* iterator) {
  FasterIterator<Key, Value, disk_t>* fasterIterator = static_cast<FasterIterator<Key, Value, disk_t>*>(iterator);
  delete(fasterIterator);
}

void faster_scan_in_memory_destroy_u64(void* iterator) {
  FasterIterator<U64Key, U64Value, disk_t>* fasterIterator = static_cast<FasterIterator<U64Key, U64Value, disk_t>*>(iterator);
  delete(fasterIterator);
}

void faster_scan_in_memory_destroy_u64_pair(void* iterator) {
  FasterIterator<U64Key, U64PairValue, disk_t>* fasterIterator = static_cast<FasterIterator<U64Key, U64PairValue, disk_t>*>(iterator);
  delete(fasterIterator);
}

void* faster_scan_in_memory_record_init() {
  return new FasterIteratorRecord<Key, Value, disk_t>();
}

void* faster_scan_in_memory_record_init_u64() {
  return new FasterIteratorRecord<U64Key, U64Value, disk_t>();
}

void* faster_scan_in_memory_record_init_u64_pair() {
  return new FasterIteratorRecord<U64Key, U64PairValue, disk_t>();
}

void faster_scan_in_memory_record_destroy(void* record) {
  FasterIteratorRecord<Key, Value, disk_t>* fasterRecord = static_cast<FasterIteratorRecord<Key, Value, disk_t>*>(record);
  delete(fasterRecord);
}

void faster_scan_in_memory_record_destroy_u64(void* record) {
  FasterIteratorRecord<U64Key, U64Value, disk_t>* fasterRecord = static_cast<FasterIteratorRecord<U64Key, U64Value, disk_t>*>(record);
  delete(fasterRecord);
}

void faster_scan_in_memory_record_destroy_u64_pair(void* record) {
  FasterIteratorRecord<U64Key, U64PairValue, disk_t>* fasterRecord = static_cast<FasterIteratorRecord<U64Key, U64PairValue, disk_t>*>(record);
  delete(fasterRecord);
}

faster_iterator_result* faster_iterator_get_next(void* iterator, void* record) {
  FasterIterator<Key, Value, disk_t>* fasterIterator = static_cast<FasterIterator<Key, Value, disk_t>*>(iterator);
  FasterIteratorRecord<Key, Value, disk_t>* fasterRecord = static_cast<FasterIteratorRecord<Key, Value, disk_t>*>(record);
  bool status = fasterIterator->GetNext(fasterRecord);
  faster_iterator_result* res = (faster_iterator_result*) malloc(sizeof(faster_iterator_result));
  res->status = status;
  res->key = fasterRecord->key()->clone();
  res->key_length = fasterRecord->key()->length();
  res->value = fasterRecord->value()->clone();
  res->value_length = fasterRecord->value()->length();
  return res;
}

faster_iterator_result_u64* faster_iterator_get_next_u64(void* iterator, void* record) {
  FasterIterator<U64Key, U64Value, disk_t>* fasterIterator = static_cast<FasterIterator<U64Key, U64Value, disk_t>*>(iterator);
  FasterIteratorRecord<U64Key, U64Value, disk_t>* fasterRecord = static_cast<FasterIteratorRecord<U64Key, U64Value, disk_t>*>(record);
  bool status = fasterIterator->GetNext(fasterRecord);
  faster_iterator_result_u64* res = (faster_iterator_result_u64*) malloc(sizeof(faster_iterator_result));
  res->status = status;
  res->key = fasterRecord->key()->key();
  res->value = fasterRecord->value()->value();
  return res;
}

faster_iterator_result_u64_pair* faster_iterator_get_next_u64_pair(void* iterator, void* record) {
  FasterIterator<U64Key, U64PairValue, disk_t>* fasterIterator = static_cast<FasterIterator<U64Key, U64PairValue, disk_t>*>(iterator);
  FasterIteratorRecord<U64Key, U64PairValue, disk_t>* fasterRecord = static_cast<FasterIteratorRecord<U64Key, U64PairValue, disk_t>*>(record);
  bool status = fasterIterator->GetNext(fasterRecord);
  faster_iterator_result_u64_pair* res = (faster_iterator_result_u64_pair*) malloc(sizeof(faster_iterator_result));
  res->status = status;
  if (status) {
    res->key = fasterRecord->key()->key();
    res->left = fasterRecord->value()->left();
    res->right = fasterRecord->value()->right();
  }
  return res;
}

void faster_iterator_result_destroy(faster_iterator_result* result) {
  free(result->key);
  free(result->value);
  free(result);
}

void faster_iterator_result_destroy_u64(faster_iterator_result_u64* result) {
  free(result);
}

void faster_iterator_result_destroy_u64_pair(faster_iterator_result_u64_pair* result) {
  free(result);
}

  // It is up to the caller to dealloc faster_checkpoint_result*
  // first token, then struct
  faster_checkpoint_result* faster_checkpoint(faster_t* faster_t) {
    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
      assert(result == Status::Ok);
    };

    Guid token;
    bool checked;
    switch (faster_t->type) {
      case NULL_DISK:
        checked = faster_t->obj.null_store->Checkpoint(nullptr, hybrid_log_persistence_callback, token);
        break;
      case FILESYSTEM_DISK:
        checked = faster_t->obj.store->Checkpoint(nullptr, hybrid_log_persistence_callback, token);
        break;
    }
    faster_checkpoint_result* res = (faster_checkpoint_result*) malloc(sizeof(faster_checkpoint_result));
    res->checked = checked;
    res->token = (char*) malloc(37 * sizeof(char));
    strncpy(res->token, token.ToString().c_str(), 37);
    return res;
  }

  // It is up to the caller to dealloc faster_checkpoint_result*
  // first token, then struct
  faster_checkpoint_result* faster_checkpoint_index(faster_t* faster_t) {
    auto index_persistence_callback = [](Status result) {
        assert(result == Status::Ok);
    };

    Guid token;
    bool checked;
    switch (faster_t->type) {
      case NULL_DISK:
        checked = faster_t->obj.null_store->CheckpointIndex(index_persistence_callback, token);
        break;
      case FILESYSTEM_DISK:
        checked = faster_t->obj.store->CheckpointIndex(index_persistence_callback, token);
        break;
    }
    faster_checkpoint_result* res = (faster_checkpoint_result*) malloc(sizeof(faster_checkpoint_result));
    res->checked = checked;
    res->token = (char*) malloc(37 * sizeof(char));
    strncpy(res->token, token.ToString().c_str(), 37);
    return res;
  }

  // It is up to the caller to dealloc faster_checkpoint_result*
  // first token, then struct
  faster_checkpoint_result* faster_checkpoint_hybrid_log(faster_t* faster_t) {
    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
        assert(result == Status::Ok);
    };

    Guid token;
    bool checked;
    switch (faster_t->type) {
      case NULL_DISK:
        checked = faster_t->obj.null_store->CheckpointHybridLog(hybrid_log_persistence_callback, token);
        break;
      case FILESYSTEM_DISK:
        checked = faster_t->obj.store->CheckpointHybridLog(hybrid_log_persistence_callback, token);
        break;
    }
    faster_checkpoint_result* res = (faster_checkpoint_result*) malloc(sizeof(faster_checkpoint_result));
    res->checked = checked;
    res->token = (char*) malloc(37 * sizeof(char));
    strncpy(res->token, token.ToString().c_str(), 37);
    return res;
  }

  void faster_destroy(faster_t *faster_t) {
    if (faster_t == NULL)
      return;

    switch (faster_t->type) {
      case NULL_DISK:
        delete faster_t->obj.null_store;
        break;
      case FILESYSTEM_DISK:
        delete faster_t->obj.store;
        break;
      case AUCTIONS_STORE:
        delete faster_t->obj.auctions_store;
        break;
      case PERSON_STORE:
        delete faster_t->obj.people_store;
        break;
      case U64_STORE:
        delete faster_t->obj.u64_store;
        break;
      case U64_PAIR_STORE:
        delete faster_t->obj.u64_pair_store;
        break;
      case TEN_ELEMENTS_STORE:
        delete faster_t->obj.ten_elements_store;
        break;
      case AUCTION_BIDS_STORE:
        delete faster_t->obj.auction_bids_store;
        break;
    }
    delete faster_t;
  }

  uint64_t faster_size(faster_t* faster_t) {
    if (faster_t == NULL) {
      return -1;
    } else {
      switch (faster_t->type) {
        case NULL_DISK:
          return faster_t->obj.null_store->Size();
        case FILESYSTEM_DISK:
          return faster_t->obj.store->Size();
        case PERSON_STORE:
          return faster_t->obj.people_store->Size();
        case AUCTIONS_STORE:
          return faster_t->obj.auctions_store->Size();
        case U64_STORE:
          return faster_t->obj.u64_store->Size();
        case U64_PAIR_STORE:
          return faster_t->obj.u64_pair_store->Size();
        case TEN_ELEMENTS_STORE:
          return faster_t->obj.ten_elements_store->Size();
        case AUCTION_BIDS_STORE:
          return faster_t->obj.auction_bids_store->Size();
      }
    }
  }

  // It is up to the caller to deallocate the faster_recover_result* struct
  faster_recover_result* faster_recover(faster_t* faster_t, const char* index_token, const char* hybrid_log_token) {
    if (faster_t == NULL) {
      return NULL;
    } else {
      uint32_t ver;
      std::vector<Guid> _session_ids;

      std::string index_str(index_token);
      std::string hybrid_str(hybrid_log_token);
      //TODO: error handling
      Guid index_guid = Guid::Parse(index_str);
      Guid hybrid_guid = Guid::Parse(hybrid_str);
      Status sres;
      switch (faster_t->type) {
        case NULL_DISK:
          sres = faster_t->obj.null_store->Recover(index_guid, hybrid_guid, ver, _session_ids);
          break;
        case FILESYSTEM_DISK:
          sres = faster_t->obj.store->Recover(index_guid, hybrid_guid, ver, _session_ids);
          break;
      }

      uint8_t status_result = static_cast<uint8_t>(sres);
      faster_recover_result* res = (faster_recover_result*) malloc(sizeof(faster_recover_result));
      res->status= status_result;
      res->version = ver;

      int ids_total = _session_ids.size();
      res->session_ids_count = ids_total;
      int session_len = 37; // 36 + 1
      res->session_ids = (char*) malloc(sizeof(char) * ids_total * session_len);

      int counter = 0;
      for (auto& id : _session_ids) {
        strncpy(res->session_ids + counter * session_len * sizeof(char), id.ToString().c_str(), session_len);
        counter++;
      }

      return res;
    }
  }

  void faster_complete_pending(faster_t* faster_t, bool b) {
    if (faster_t != NULL) {
      switch (faster_t->type) {
        case NULL_DISK:
          faster_t->obj.null_store->CompletePending(b);
          break;
        case FILESYSTEM_DISK:
          faster_t->obj.store->CompletePending(b);
          break;
        case PERSON_STORE:
          faster_t->obj.people_store->CompletePending(b);
          break;
        case AUCTIONS_STORE:
          faster_t->obj.auctions_store->CompletePending(b);
          break;
        case U64_STORE:
          faster_t->obj.u64_store->CompletePending(b);
          break;
        case U64_PAIR_STORE:
          faster_t->obj.u64_pair_store->CompletePending(b);
          break;
        case TEN_ELEMENTS_STORE:
          faster_t->obj.ten_elements_store->CompletePending(b);
          break;
        case AUCTION_BIDS_STORE:
          faster_t->obj.auction_bids_store->CompletePending(b);
          break;
      }
    }
  }

  // Thread-related

  const char* faster_start_session(faster_t* faster_t) {
    if (faster_t == NULL) {
      return NULL;
    } else {
      Guid guid;
      switch (faster_t->type) {
        case NULL_DISK:
          guid = faster_t->obj.null_store->StartSession();
          break;
        case FILESYSTEM_DISK:
          guid = faster_t->obj.store->StartSession();
          break;
        case PERSON_STORE:
          guid = faster_t->obj.people_store->StartSession();
          break;
        case AUCTIONS_STORE:
          guid = faster_t->obj.auctions_store->StartSession();
          break;
        case U64_STORE:
          guid = faster_t->obj.u64_store->StartSession();
          break;
        case U64_PAIR_STORE:
          guid = faster_t->obj.u64_pair_store->StartSession();
          break;
        case TEN_ELEMENTS_STORE:
          guid = faster_t->obj.ten_elements_store->StartSession();
          break;
        case AUCTION_BIDS_STORE:
          guid = faster_t->obj.auction_bids_store->StartSession();
          break;
      }
      char* str = new char[37];
      std::strcpy(str, guid.ToString().c_str());
      return str;
    }

  }

  uint64_t faster_continue_session(faster_t* faster_t, const char* token) {
    if (faster_t == NULL) {
      return -1;
    } else {
      std::string guid_str(token);
      Guid guid = Guid::Parse(guid_str);
      switch (faster_t->type) {
        case NULL_DISK:
          return faster_t->obj.null_store->ContinueSession(guid);
        case FILESYSTEM_DISK:
          return faster_t->obj.store->ContinueSession(guid);
      }
    }
  }

  void faster_stop_session(faster_t* faster_t) {
    if (faster_t != NULL) {
      switch (faster_t->type) {
        case NULL_DISK:
          faster_t->obj.null_store->StopSession();
          break;
        case FILESYSTEM_DISK:
          faster_t->obj.store->StopSession();
          break;
      }
    }
  }

  void faster_refresh_session(faster_t* faster_t) {
    if (faster_t != NULL) {
      switch (faster_t->type) {
        case NULL_DISK:
          faster_t->obj.null_store->Refresh();
          break;
        case FILESYSTEM_DISK:
          faster_t->obj.store->Refresh();
          break;
        case PERSON_STORE:
          faster_t->obj.people_store->Refresh();
          break;
        case AUCTIONS_STORE:
          faster_t->obj.auctions_store->Refresh();
          break;
        case U64_STORE:
          faster_t->obj.u64_store->Refresh();
          break;
        case U64_PAIR_STORE:
          faster_t->obj.u64_pair_store->Refresh();
          break;
        case TEN_ELEMENTS_STORE:
          faster_t->obj.ten_elements_store->Refresh();
          break;
        case AUCTION_BIDS_STORE:
          faster_t->obj.auction_bids_store->Refresh();
          break;
      }
    }
  }

  void faster_dump_distribution(faster_t* faster_t) {
    if (faster_t != NULL) {
      switch (faster_t->type) {
        case NULL_DISK:
          faster_t->obj.null_store->DumpDistribution();
          break;
        case FILESYSTEM_DISK:
          faster_t->obj.store->DumpDistribution();
          break;
      }
    }
  }

  bool faster_grow_index(faster_t* faster_t) {
    auto grow_index_callback = [](uint64_t new_size) {
        assert(new_size > 0);
    };
    if (faster_t != NULL) {
      switch (faster_t->type) {
        case NULL_DISK:
          return faster_t->obj.null_store->GrowIndex(grow_index_callback);
        case FILESYSTEM_DISK:
          return faster_t->obj.store->GrowIndex(grow_index_callback);
      }
    }
  }

} // extern "C"
