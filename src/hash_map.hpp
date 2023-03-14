#ifndef LSMTREE_HASH_MAP_HPP
#define LSMTREE_HASH_MAP_HPP

#include <algorithm>
#include <array>
#include <climits>

#include "murmur3.hpp"
#include "run.hpp"

template <typename K, typename V>
class HashTable {
 private:
  kvPair<K, V> *Table;

 public:
  long _size;
  long _elts;

  kvPair<K, V> DEFAULT = {INT_MIN, INT_MAX};

  HashTable(long size) : _size(size), _elts(0) {
    Table = new kvPair<K, V>[_size]();
    std::fill(Table, Table + _size, (kvPair<K, V>)DEFAULT);
  }

  ~HashTable() { delete[] Table; }

  long hashFunc(const K key) {
    std::array<long, 2> hashValue;

    MurmurHash3_x64_128(&key, sizeof(K), 0, hashValue.data());
    return (hashValue[0] % _size);
  }

  void resize() {
    _size *= 2;
    auto NTable = new kvPair<K, V>[_size]();
    std::fill(NTable, NTable + _size, (kvPair<K, V>)DEFAULT);

    for (auto i = 0; i < _size / 2; i++) {
      if (Table[i] != DEFAULT) {
        long hashValue = hashFunc(Table[i].key);

        for (auto j = 0;; j++) {
          if (NTable[(hashValue + j) % _size] == DEFAULT) {
            NTable[(hashValue + j) % _size] = Table[i];
            break;
          }
        }
      }
    }
    delete[] Table;
    Table = NTable;
  }

  bool get(const K &key, V &value) {
    long hashValue = hashFunc(key);
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
    long hashValue = hashFunc(key);

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

  V putIfEmpty(const K &key, const V &value) {
    if (_elts * 2 > _size) {
      resize();
    }

    long hashValue = hashFunc(key);

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