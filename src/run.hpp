#ifndef LSMTREE_OPT_HPP
#define LSMTREE_OPT_HPP

#include <vector>

typedef long long LL;
typedef unsigned long UL;
typedef unsigned long long ULL;

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

// Run 代表了 LSM Tree 的执行单元
template <class K, class V>
class Run {
 public:
  virtual K GetMin() = 0;
  virtual K GetMax() = 0;
  virtual void InsertKey(const K &key, const V &value) = 0;
  virtual void DeleteKey(const K &key) = 0;
  virtual V Search(const K &key, bool &isFound) = 0;
  virtual ULL NumElements() = 0;
  virtual void SetSize(unsigned long size) = 0;
  virtual std::vector<kvPair<K, V>> GetAll() = 0;
  virtual std::vector<kvPair<K, V>> GetAllInRange(const K &k1, const K &k2) = 0;
  virtual ~Run() = default;
};

#endif  // LSMTREE_OPT_HPP
