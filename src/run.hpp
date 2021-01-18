#ifndef LSMTREE_OPT_HPP
#define LSMTREE_OPT_HPP

#include <vector>

template <typename K, typename V>
class kvPair {
 public:
  K key;
  V value;
  bool operator==(kvPair kv) const {
    return kv.key == key && kv.value == value;
  }
  bool operator!=(kvPair kv) const {
    return kv.key != key || kv.value != value;
  }
  bool operator<(kvPair kv) const { return key < kv.key; }
  bool operator>(kvPair kv) const { return key > kv.key; }
};

// Run 是 LSM 的最小单元
template <class K, class V>
class Run {
 public:
  virtual K getMin() = 0;
  virtual K getMax() = 0;
  virtual void insertKey(const K &key, const V &value) = 0;
  virtual void deleteKey(const K &key) = 0;
  virtual V search(const K &key, bool &isFound) = 0;
  virtual long long eltsNums() = 0;
  virtual void setSize(const long size) = 0;
  virtual std::vector<kvPair<K, V>> getAll() = 0;
  virtual std::vector<kvPair<K, V>> getAllInRange(const K &k1, const K &k2) = 0;
  virtual ~Run() = default;
};

#endif  // LSMTREE_OPT_HPP
