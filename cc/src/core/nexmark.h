//
// Created by matt on 06/09/19.
//

#include <stddef.h>

#ifndef FASTER_NEXMARK_H
#define FASTER_NEXMARK_H
struct person {
    const char* name;
    const char* city;
    const char* state;
    size_t name_length;
    size_t city_length;
    size_t state_length;
};

typedef struct person person_t;

struct auction {
    size_t id;
    size_t category;
    size_t date_time;
    size_t expires;
    size_t reserve;
};

typedef struct auction auction_t;

struct bid {
    size_t price;
    size_t bidder;
    size_t date_time;
};

typedef struct bid bid_t;

#endif //FASTER_NEXMARK_H
