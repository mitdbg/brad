#include "radixspline.hpp"
#include <iostream>

int32_t add(int32_t a, int32_t b) {
    return a + b;
}

void* build(uint64_t* ks, uint64_t size) {
    std::cout << "flag" << std::endl;
    // RSData* rs = (RSData*)malloc(sizeof(RSData));
    RSData* rs = new RSData;
    std::cout << "flag 2" << std::endl;
    // rs->keys = std::vector<uint64_t>(ks, ks + size);
    rs->keys = std::vector<uint64_t>(size);
    memcpy(rs->keys.data(), ks, size * sizeof(uint64_t));
    uint64_t min = rs->keys.front();
    uint64_t max = rs->keys.back();
    rs::Builder<uint64_t> rsb(min, max);
    for (const auto& key : rs->keys) rsb.AddKey(key);
    rs::RadixSpline<uint64_t> rso = rsb.Finalize();
    // memcpy(&(rs->rspline), &rso, sizeof(rs::RadixSpline<uint64_t>));
    rs->rspline = rso;
    return (void*)rs;
}

bool lookup(void* ptr, uint64_t key) {
    RSData* rs = (RSData*) ptr; 
    rs::SearchBound bound = rs->rspline.GetSearchBound(key);
    auto start = begin(rs->keys) + bound.begin, last = begin(rs->keys) + bound.end;
    auto iter = std::lower_bound(start, last, key);
    return iter != rs->keys.end() && *iter == key;
}

void clear(void* ptr) {
    RSData* rs = (RSData*) ptr;
    free(rs);
}