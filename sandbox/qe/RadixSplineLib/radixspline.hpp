#include "../RadixSpline/include/rs/builder.h"
#include <string.h>


struct RSData {
    std::vector<uint64_t> keys;
    rs::RadixSpline<uint64_t> rspline;
};

extern "C" {

    int32_t add(int32_t a, int32_t b);
    void* build(uint64_t* ks, uint64_t size);

    bool lookup(void* ptr, uint64_t key);

    void clear(void* ptr);
}