//
// Created by abcdlsj on 2021/1/8.
//

#ifndef LSMTREE_BLOOM_FILTER_HPP
#define LSMTREE_BLOOM_FILTER_HPP

#include <array>
#include <cmath>
#include <vector>

#include "murmur3.hpp"

// <https://findingprotopia.org/posts/how-to-write-a-bloom-filter-cpp/>

template <class Key>
class BloomFilter {
 public:
  BloomFilter(uint64_t _n, double _p) {
    double m = -1 * static_cast<double>(_n) * log(_p) /
               0.480453013918201;            // 0.480453013918201 = ln(2) ^ 2;
    k = ceil((m / _n) * 0.693147180559945);  // 0.693147180559945 = ln(2);
    b = std::vector<bool>((int)m);
  }

  std::array<uint64_t, 2> hash(const Key *data, size_t len) {
    std::array<uint64_t, 2> hashValue;
    MurmurHash3_x64_128(data, static_cast<int>(len), 0, hashValue.data());
    return hashValue;
  }

  inline uint64_t nthHash(uint32_t n, uint64_t hashA, uint64_t hashB,
                          uint64_t filterSize) {
    return (hashA + n * hashB) % filterSize;
  }

  void add(const Key *data, std::size_t len) {
    auto hashValues = hash(data, len);

    for (int n = 0; n < k; n++) {
      b[nthHash(n, hashValues[0], hashValues[1], b.size())] = true;
    }
  }

  bool isContain(const Key *data, std::size_t len) {
    auto hashValues = hash(data, len);

    for (int n = 0; n < k; n++) {
      if (!b[nthHash(n, hashValues[0], hashValues[1], b.size())]) {
        return false;
      }
    }

    return true;
  }

 private:
  std::vector<bool> b;
  uint8_t k;
};

#endif  // LSMTREE_BLOOM_FILTER_HPP
