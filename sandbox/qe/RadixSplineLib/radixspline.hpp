#include "../RadixSpline/include/rs/builder.h"
#include <string.h>


struct RSData {
    std::vector<uint64_t> keys;
    rs::RadixSpline<uint64_t> rspline;
};

void* build(std::vector<uint64_t> ks);

bool lookup(void* ptr, uint64_t key);

void clear(void* ptr);