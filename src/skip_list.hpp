#ifndef LSMTREE_SKIP_LIST_HPP
#define LSMTREE_SKIP_LIST_HPP

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>

#include "run.hpp"

std::default_random_engine generator;
std::uniform_real_distribution<double> distribution(0.0, 1.0);

template <class K, class V, int MAXLEVEL>
class SNode {
 public:
  const K key;
  V value;
  SNode<K, V, MAXLEVEL> *_forward[MAXLEVEL + 1];
  SNode(const K _key) : key(_key) {
    for (int i = 1; i <= MAXLEVEL; i++) {
      _forward[i] = nullptr;
    }
  }
  SNode(const K _key, V _value) : key(_key), value(_value) {
    for (int i = 1; i <= MAXLEVEL; i++) {
      _forward[i] = nullptr;
    }
  }
  virtual ~SNode(){};
};

template <class K, class V, int MAXLEVEL = 20>
class SkipList : public Run<K, V> {
 public:
  typedef SNode<K, V, MAXLEVEL> Node;
  const int maxLevel;
  K _min, _max;

  K minKey, maxKey;
  long long _n;
  size_t _maxSize;
  int curMaxLevel;
  Node *p_listHead, *p_listTail;

  SkipList(const K minKey, const K maxKey)
      : maxLevel(MAXLEVEL),
        _min(static_cast<K>(NULL)),
        _max(static_cast<K>(NULL)),
        minKey(minKey),
        maxKey(maxKey),
        _n(0),
        curMaxLevel(1),
        p_listHead(nullptr),
        p_listTail(nullptr) {
    p_listHead = new Node(minKey);
    p_listTail = new Node(maxKey);
    for (int i = 1; i <= maxLevel; i++) {
      p_listHead->_forward[i] = p_listTail;
    }
  }

  ~SkipList() {
    Node *curNode = p_listHead->_forward[1];
    while (curNode != p_listTail) {
      Node *tmp = curNode;
      curNode = curNode->_forward[1];
      delete tmp;
    }

    delete p_listHead;
    delete p_listTail;
  }

  K getMax() { return _max; }
  K getMin() { return _min; }

  void insertKey(const K &iKey, const V &iValue) {
    _max = std::max(_max, iKey);
    _min = std::min(_min, iKey);

    Node *update[MAXLEVEL], *curNode = p_listHead;
    for (int level = curMaxLevel; level > 0; level--) {
      while (curNode->_forward[level]->key < iKey) {
        curNode = curNode->_forward[level];
      }
      update[level] = curNode;
    }
    curNode = curNode->_forward[1];
    if (curNode->key == iKey) {
      curNode->value = iValue;
    } else {
      int insert_level = genNodeLevel();
      if (insert_level > curMaxLevel && insert_level < MAXLEVEL - 1) {
        for (int level = curMaxLevel + 1; level <= insert_level; level++) {
          update[level] = p_listHead;
        }
        curMaxLevel = insert_level;
      }
      curNode = new Node(iKey, iValue);
      for (int level = 1; level <= curMaxLevel; level++) {
        curNode->_forward[level] = update[level]->_forward[level];
        update[level]->_forward[level] = curNode;
      }
      ++_n;
    }
  }

  void deleteKey(const K &dKey) {
    Node *update[MAXLEVEL], *curNode = p_listHead;

    for (int level = curMaxLevel; level > 0; level--) {
      while (curNode->_forward[level]->key < dKey) {
        curNode = curNode->_forward[level];
      }
      update[level] = curNode;
    }
    curNode = curNode->_forward[1];
    if (curNode->key == dKey) {
      for (int level = 1; level <= curMaxLevel; level++) {
        if (update[level]->_forward[level] != curNode) {
          break;
        }
        update[level]->_forward[level] = curNode->_forward[level];
      }
      delete curNode;
      while (curMaxLevel > 1 && p_listHead->_forward[curMaxLevel] == nullptr) {
        curMaxLevel--;
      }
    }
    --_n;
  };

  V search(const K &sKey, bool &isFound) {
    Node *curNode = p_listHead;
    for (int level = curMaxLevel; level >= 1; level--) {
      while (curNode->_forward[level]->key < sKey) {
        curNode = curNode->_forward[level];
      }
    }
    curNode = curNode->_forward[1];
    if (curNode && curNode->key == sKey) {
      isFound = true;
      return curNode->value;
    }

    return static_cast<V>(NULL);
  };

  bool isContain(K &key) {
    bool isFound = false;
    Search(key, isFound);
    return isFound;
  }

  bool isEmpty() { return p_listHead->_forward[0] == p_listTail; }
  long long eltsNums() { return _n; }
  void setSize(const long size) { _maxSize = size; }
  size_t getBytesSize() { return _n * (sizeof(K) + sizeof(V)); }

  std::vector<kvPair<K, V>> getAll() {
    std::vector<kvPair<K, V>> ret = std::vector<kvPair<K, V>>();
    Node *node = p_listHead->_forward[1];
    while (node != p_listTail) {
      kvPair<K, V> kv = {node->key, node->value};
      ret.template emplace_back(kv);
      node = node->_forward[1];
    }
    return ret;
  }

  std::vector<kvPair<K, V>> getAllInRange(const K &k1, const K &k2) {
    if (k1 > _max || k2 < _min) {
      return {};
    }
    std::vector<kvPair<K, V>> ret = std::vector<kvPair<K, V>>();
    Node *node = p_listHead->_forward[1];
    while (node->key < k1) {
      node = node->_forward[1];
    }
    while (node->key < k2) {
      kvPair<K, V> kv = {node->key, node->value};
      ret.template emplace_back(kv);
      node = node->_forward[1];
    }

    return ret;
  }

  int genNodeLevel() { return ffs(rand() & ((1 << MAXLEVEL) - 1)) - 1; }
};

#endif  // LSMTREE_SKIP_LIST_HPP