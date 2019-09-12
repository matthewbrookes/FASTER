#include "gtest/gtest.h"

#include "core/faster.h"
#include "device/null_disk.h"

using namespace FASTER::core;
TEST(Iterator, IterateU8) {
  class alignas(2) Key {
  public:
      Key(uint8_t key)
              : key_{ key } {
      }

      inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Key));
      }
      inline KeyHash GetHash() const {
        std::hash<uint8_t> hash_fn;
        return KeyHash{ hash_fn(key_) };
      }

      inline uint8_t key() const {
        return key_;
      }

      /// Comparison operators.
      inline bool operator==(const Key& other) const {
        return key_ == other.key_;
      }
      inline bool operator!=(const Key& other) const {
        return key_ != other.key_;
      }

  private:
      uint8_t key_;
  };

  class UpsertContext;
  class ReadContext;

  class Value {
  public:
      Value()
              : value_{ 0 } {
      }
      Value(const Value& other)
              : value_{ other.value_ } {
      }
      Value(uint8_t value)
              : value_{ value } {
      }

      inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Value));
      }

      inline uint8_t value() const {
        return atomic_value_.load();
      }

      friend class UpsertContext;
      friend class ReadContext;

  private:
      union {
          uint8_t value_;
          std::atomic<uint8_t> atomic_value_;
      };
  };

  class UpsertContext : public IAsyncContext {
  public:
      typedef Key key_t;
      typedef Value value_t;

      UpsertContext(uint8_t key)
              : key_{ key } {
      }

      /// Copy (and deep-copy) constructor.
      UpsertContext(const UpsertContext& other)
              : key_{ other.key_ } {
      }

      /// The implicit and explicit interfaces require a key() accessor.
      inline const Key& key() const {
        return key_;
      }
      inline static constexpr uint32_t value_size() {
        return sizeof(value_t);
      }
      /// Non-atomic and atomic Put() methods.
      inline void Put(Value& value) {
        value.value_ = 23;
      }
      inline bool PutAtomic(Value& value) {
        value.atomic_value_.store(42);
        return true;
      }

  protected:
      /// The explicit interface requires a DeepCopy_Internal() implementation.
      Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }

  private:
      Key key_;
  };

  class ReadContext : public IAsyncContext {
  public:
      typedef Key key_t;
      typedef Value value_t;

      ReadContext(uint8_t key)
              : key_{ key } {
      }

      /// Copy (and deep-copy) constructor.
      ReadContext(const ReadContext& other)
              : key_{ other.key_ } {
      }

      /// The implicit and explicit interfaces require a key() accessor.
      inline const Key& key() const {
        return key_;
      }

      inline void Get(const Value& value) {
        // All reads should be atomic (from the mutable tail).
        ASSERT_TRUE(false);
      }
      inline void GetAtomic(const Value& value) {
        output = value.atomic_value_.load();
      }

  protected:
      /// The explicit interface requires a DeepCopy_Internal() implementation.
      Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }

  private:
      Key key_;
  public:
      uint8_t output;
  };

  FasterKv<Key, Value, FASTER::device::NullDisk> store { 128, 1073741824, "" };

  store.StartSession();

  // Insert.
  for(size_t idx = 0; idx < 256; ++idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        // In-memory test.
        ASSERT_TRUE(false);
    };
    UpsertContext context{ static_cast<uint8_t>(idx) };
    Status result = store.Upsert(context, callback, 1);
    ASSERT_EQ(Status::Ok, result);
  }

  FasterIterator<Key, Value, FASTER::device::NullDisk>* iterator = store.ScanInMemory();
  auto iteratorRecord = new FasterIteratorRecord<Key, Value, FASTER::device::NullDisk>();
  uint8_t expected_key = 0;
  while (iterator->GetNext(iteratorRecord)) {
    const Key* key = iteratorRecord->key();
    const Value* value = iteratorRecord->value();
    ASSERT_EQ(expected_key++, key->key());
    ASSERT_EQ(23, value->value());
  }
  delete iterator;
  delete iteratorRecord;

  store.StopSession();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
