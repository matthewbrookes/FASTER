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

#endif //FASTER_NEXMARK_H
