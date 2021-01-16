#ifndef LSMTREE_HASH_MAP_HPP
#define LSMTREE_HASH_MAP_HPP

#include <array>
#include <climits>
#include <vector>

#include "murmur3.hpp"
#include "run.hpp"

template <typename K, typename V>
class HashTable {
 private:
  std::vector<kvPair<K, V>> Table;

 public:
  UL _size;
  UL _elts;
  kvPair<K, V> DEFAULT = {INT_MIN, INT_MAX};
  HashTable(UL size) : _size(size), _elts(0) {
    Table = new std::vector<kvPair<K, V>>(_size, DEFAULT);
  }
  ~HashTable() { delete[] Table; }
  unsigned long hashFunc(const K key) {
    std::array<unsigned long, 2> hashValue;

    MurmurHash3_x64_128(&key, sizeof(K), 0, hashValue.data());
    return (hashValue[0] % _size);
  }

  void resize() {
    _size *= 2;
    auto NTable = new std::vector<kvPair<K, V>>(_size, DEFAULT);

    for (auto i = 0; i < _size / 2; i++) {
      if (Table[i] != DEFAULT) {
        UL hashValue = hashFunc(Table[i].key);

        for (auto j = 0;; j++) {
          if (NTable[(hashValue + i) % _size] == DEFAULT) {
            NTable[(hashValue + i) % _size] = Table[i];
            break;
          }
        }
      }
    }
    delete[] Table;
    Table = NTable;
  }

  bool get(const K &key, V &value) {
    UL hashValue = hashFunc(key);
    for (auto i = 0;; i++) {
      if (Table[(hashValue + i) % _size] == DEFAULT) {
        return false;
      } else if (Table[(hashValue + i) % _size].key == key) {
        value = Table[(hashValue + i) % _size].value;
        return true;
      }
    }

    return false;
  }

  void put(const K &key, const V &value) {
    UL hashValue = hashFunc(key);

    for (auto i = 0;; i++) {
      if (Table[(hashValue + i) % _size] == DEFAULT) {
        Table[(hashValue + i) % _size].key = key;
        Table[(hashValue + i) % _size].value = value;
        ++_elts;
        return;
      } else if (Table[(hashValue + i) % _size].key == key) {
        Table[(hashValue + i) % _size].value = value;
        return;
      }
    }
  }

  V putAndReturnValue(const K &key, const V &value) {
    if (_elts * 2 > _size) {
      resize();
    }

    UL hashValue = hashFunc(key);

    for (auto i = 0;; i++) {
      if (Table[(hashValue + i) % _size] == DEFAULT) {
        Table[(hashValue + i) % _size].key = key;
        Table[(hashValue + i) % _size].value = value;
        ++_elts;
        return static_cast<V>(NULL);
      } else if (Table[(hashValue + i) % _size].key == key) {
        return Table[(hashValue + i) % _size].value = value;
      }
    }

    return static_cast<V>(NULL);
  }
};

#endif  // LSMTREE_HASH_MAP_HPP