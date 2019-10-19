// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

// C interface for the C++ code


#ifndef FASTER_C_H_
#define FASTER_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "nexmark.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

  typedef struct faster_t faster_t;

  typedef struct faster_result faster_result;
  typedef void (*faster_callback)(faster_result);

  enum faster_status {
      Ok,
      Pending,
      NotFound,
      OutOfMemory,
      IOError,
      Corrupted,
      Aborted
  };
  typedef enum faster_status faster_status;

  typedef void (*read_callback)(void*, const uint8_t*, uint64_t, faster_status);
  typedef void (*read_person_callback)(void*, const person_t, faster_status);
  typedef void (*read_auctions_callback)(void*, const uint64_t*, uint64_t, faster_status);
  typedef void (*read_u64_callback)(void*, const uint64_t, faster_status);
  typedef void (*read_u64_pair_callback)(void*, uint64_t*, uint64_t*, faster_status);
  typedef void (*read_ten_elements_callback)(void*, size_t, faster_status);
  typedef void (*read_auction_bids_callback)(void*, const auction_t*, bid_t*, const size_t, faster_status);
  typedef uint64_t (*rmw_callback)(const uint8_t*, uint64_t, uint8_t*, uint64_t, uint8_t*);

  typedef struct faster_checkpoint_result faster_checkpoint_result;
  struct faster_checkpoint_result {
    bool checked;
    char* token;
  };

  typedef struct faster_recover_result faster_recover_result;
  struct faster_recover_result {
    uint8_t status;
    uint32_t version;
    int session_ids_count;
    char* session_ids;
  };

  typedef struct faster_iterator_result faster_iterator_result;
  struct faster_iterator_result {
      bool status;
      uint8_t* key;
      uint64_t key_length;
      uint8_t* value;
      uint64_t value_length;
  };

  typedef struct faster_iterator_result_u64 faster_iterator_result_u64;
  struct faster_iterator_result_u64 {
      bool status;
      uint64_t key;
      uint64_t value;
  };

typedef struct faster_iterator_result_u64_pair faster_iterator_result_u64_pair;
struct faster_iterator_result_u64_pair {
    bool status;
    uint64_t key;
    uint64_t left;
    uint64_t right;
};

  // Thread-related operations
  const char* faster_start_session(faster_t* faster_t);
  uint64_t faster_continue_session(faster_t* faster_t, const char* token);
  void faster_stop_session(faster_t* faster_t);
  void faster_refresh_session(faster_t* faster_t);
  void faster_complete_pending(faster_t* faster_t, bool b);

  // Checkpoint/Recover
  faster_checkpoint_result* faster_checkpoint(faster_t* faster_t);
  faster_checkpoint_result* faster_checkpoint_index(faster_t* faster_t);
  faster_checkpoint_result* faster_checkpoint_hybrid_log(faster_t* faster_t);
  faster_recover_result* faster_recover(faster_t* faster_t, const char* index_token, const char* hybrid_log_token);

  // Operations
  faster_t* faster_open(const uint64_t table_size, const uint64_t log_size);
  faster_t* faster_open_with_disk(const uint64_t table_size, const uint64_t log_size, const char* storage);
  faster_t* faster_open_with_disk_people(const uint64_t table_size, const uint64_t log_size, const char* storage);
  faster_t* faster_open_with_disk_auctions(const uint64_t table_size, const uint64_t log_size, const char* storage);
  faster_t* faster_open_with_disk_u64(const uint64_t table_size, const uint64_t log_size, const char* storage);
  faster_t* faster_open_with_disk_u64_pair(const uint64_t table_size, const uint64_t log_size, const char* storage);
faster_t* faster_open_with_disk_composite_u64(const uint64_t table_size, const uint64_t log_size, const char* storage);
faster_t* faster_open_with_disk_ten_elements(const uint64_t table_size, const uint64_t log_size, const char* storage);
faster_t* faster_open_with_disk_auction_bids(const uint64_t table_size, const uint64_t log_size, const char* storage);
  uint8_t faster_upsert(faster_t* faster_t, const uint8_t* key, const uint64_t key_length,
                        uint8_t* value, uint64_t value_length, const uint64_t monotonic_serial_number);
  uint8_t faster_upsert_person(faster_t* faster_t, const uint64_t key, person_t person, const uint64_t monotonic_serial_number);
  uint8_t faster_upsert_auctions(faster_t* faster_t, const uint64_t key, uint64_t* input, uint64_t length, const uint64_t monotonic_serial_number);
  uint8_t faster_upsert_u64(faster_t* faster_t, const uint64_t key, const uint64_t input, const uint64_t monotonic_serial_number);
  uint8_t faster_upsert_u64_pair(faster_t* faster_t, const uint64_t key, const uint64_t left, const uint64_t right, const uint64_t monotonic_serial_number);
  uint8_t faster_rmw(faster_t* faster_t, const uint8_t* key, const uint64_t key_length, uint8_t* modification,
                     const uint64_t length, const uint64_t monotonic_serial_number, rmw_callback cb);
  uint8_t faster_rmw_auction(faster_t* faster_t, const uint64_t key,
                             const uint64_t modification, const uint64_t monotonic_serial_number);
  uint8_t faster_rmw_auctions(faster_t* faster_t, const uint64_t key, uint64_t* modification,
                              uint64_t length, const uint64_t monotonic_serial_number);
  uint8_t faster_rmw_u64(faster_t* faster_t, const uint64_t key, uint64_t modification, const uint64_t monotonic_serial_number);
  uint8_t faster_rmw_u64_pair(faster_t* faster_t, const uint64_t key, uint64_t left, uint64_t right, const uint64_t monotonic_serial_number);
  uint8_t faster_rmw_decrease_u64(faster_t* faster_t, const uint64_t key, uint64_t modification, const uint64_t monotonic_serial_number);
uint8_t faster_rmw_composite_u64(faster_t* faster_t, const uint64_t left, const uint64_t right, uint64_t modification, const uint64_t monotonic_serial_number);
uint8_t faster_rmw_ten_elements(faster_t* faster_t, const uint64_t key, size_t modification, const uint64_t monotonic_serial_number);
uint8_t faster_rmw_auction_bids_auction(faster_t* faster_t, const uint64_t key, auction_t modification, const uint64_t monotonic_serial_number);
uint8_t faster_rmw_auction_bids_bid(faster_t* faster_t, const uint64_t key, bid_t modification, const uint64_t monotonic_serial_number);
  uint8_t faster_read(faster_t* faster_t, const uint8_t* key, const uint64_t key_length,
                       const uint64_t monotonic_serial_number, read_callback cb, void* target);
  uint8_t faster_read_auctions(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number,
                       read_auctions_callback cb, void* target);
  uint8_t faster_read_person(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_person_callback cb, void* target);
  uint8_t faster_read_u64(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_u64_callback cb, void* target);
uint8_t faster_read_u64_composite(faster_t* faster_t, const uint64_t left, const uint64_t right, const uint64_t monotonic_serial_number, read_u64_callback cb, void* target);
  uint8_t faster_read_u64_pair(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_u64_pair_callback cb, void* target);
uint8_t faster_read_ten_elements(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_ten_elements_callback cb, void* target);
uint8_t faster_read_auction_bids(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number, read_auction_bids_callback cb, void* target);
  uint8_t faster_delete(faster_t* faster_t, const uint8_t* key, const uint64_t key_length,
                        const uint64_t monotonic_serial_number);
  uint8_t faster_delete_u64(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number);
uint8_t faster_delete_auctions(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number);
uint8_t faster_delete_auction_bids(faster_t* faster_t, const uint64_t key, const uint64_t monotonic_serial_number);
uint8_t faster_delete_u64_composite(faster_t* faster_t, const uint64_t left, const uint64_t right, const uint64_t monotonic_serial_number);
  void* faster_scan_in_memory_init(faster_t* faster_t);
  void* faster_scan_in_memory_init_u64(faster_t* faster_t);
void* faster_scan_in_memory_init_u64_pair(faster_t* faster_t);
  void faster_scan_in_memory_destroy(void* iterator);
  void faster_scan_in_memory_destroy_u64(void* iterator);
void faster_scan_in_memory_destroy_u64_pair(void* iterator);
  void* faster_scan_in_memory_record_init();
  void* faster_scan_in_memory_record_init_u64();
void* faster_scan_in_memory_record_init_u64_pair();
  void faster_scan_in_memory_record_destroy(void* record);
  void faster_scan_in_memory_record_destroy_u64(void* record);
void faster_scan_in_memory_record_destroy_u64_pair(void* record);
  faster_iterator_result* faster_iterator_get_next(void* iterator, void* record);
  faster_iterator_result_u64* faster_iterator_get_next_u64(void* iterator, void* record);
faster_iterator_result_u64_pair* faster_iterator_get_next_u64_pair(void* iterator, void* record);
  void faster_iterator_result_destroy(faster_iterator_result* result);
  void faster_iterator_result_destroy_u64(faster_iterator_result_u64* result);
  void faster_iterator_result_destroy_u64_pair(faster_iterator_result_u64_pair* result);
  void faster_destroy(faster_t* faster_t);
  bool faster_grow_index(faster_t* faster_t);

  // Statistics
  uint64_t faster_size(faster_t* faster_t);
  void faster_dump_distribution(faster_t* faster_t);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  /* FASTER_C_H_ */

