#ifndef LSMTREE_SKIP_LIST_HPP
#define LSMTREE_SKIP_LIST_HPP

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>

#include "opt.hpp"

std::default_random_engine generator;
std::uniform_real_distribution<double> distribution(0.0, 1.0);

template <class K, class V, unsigned MAXLEAVEL>
class SNode {
 public:
  const K key;
  V value;
  SNode<K, V, MAXLEAVEL> *_forward[MAXLEAVEL + 1];
  SNode(const K _key) : key(_key) {
    for (int i = 1; i <= MAXLEAVEL; i++) {
      _forward[i] = nullptr;
    }
  }
  SNode(const K _key, V _value) : key(_key), value(_value) {
    for (int i = 1; i <= MAXLEAVEL; i++) {
      _forward[i] = nullptr;
    }
  }
  virtual ~SNode(){};
};

template <typename K, typename V, int MAXLEVEL = 20>
class SkipList : public Opt<K, V> {
 public:
  typedef SNode<K, V, MAXLEVEL> Node;
  const int maxLevel;
  K _min, _max;

  K minKey, maxKey;
  ULL _n;
  size_t _max_size;
  int cur_max_level;
  Node *p_listHead, *p_listTail;

  SkipList(const K minKey, const K maxKey)
      : p_listHead(nullptr),
        p_listTail(nullptr),
        cur_max_level(1),
        maxLevel(MAXLEVEL),
        _min(static_cast<K>(NULL)),
        _max(static_cast<K>(NULL)),
        minKey(minKey),
        maxKey(maxKey),
        _n(0) {
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

  K GetMax() { return _max; }
  K GetMin() { return _min; }

  void InsertKey(const K &iKey, const V &iValue) {
    _max = std::max(_max, iKey);
    _min = std::min(_min, iKey);

    Node *update[MAXLEVEL], *curNode = p_listHead;
    for (int level = cur_max_level; level > 0; level--) {
      while (curNode->_forward[level]->key < iKey) {
        curNode = curNode->_forward[level];
      }
      update[level] = curNode;
    }
    curNode = curNode->_forward[1];
    if (curNode->key == iKey) {
      curNode->value = iValue;
    } else {
      int insert_level = GenNodeLevel();
      if (insert_level > cur_max_level && insert_level < MAXLEVEL - 1) {
        for (int level = cur_max_level + 1; level <= insert_level; level++) {
          update[level] = p_listHead;
        }
        cur_max_level = insert_level;
      }
      curNode = new Node(iKey, iValue);
      for (int level = 1; level <= cur_max_level; level++) {
        curNode->_forward[level] = update[level]->_forward[level];
        update[level]->_forward[level] = curNode;
      }
      ++_n;
    }
  }

  void DeleteKey(const K &dKey) {
    Node *update[MAXLEVEL], *curNode = p_listHead;

    for (int level = cur_max_level; level > 0; level--) {
      while (curNode->_forward[level]->key < dKey) {
        curNode = curNode->_forward[level];
      }
      update[level] = curNode;
    }
    curNode = curNode->_forward[1];
    if (curNode->key == dKey) {
      for (int level = 1; level <= cur_max_level; level++) {
        if (update[level]->_forward[level] != curNode) {
		  break;
        }
        update[level]->_forward[level] = curNode->_forward[level];
      }
      delete curNode;
      while (cur_max_level > 1 &&
             p_listHead->_forward[cur_max_level] == nullptr) {
        cur_max_level--;
      }
    }
    --_n;
  };

  V Search(const K &sKey, bool &isFound) {
    Node *curNode = p_listHead;
    for (int level = cur_max_level; level >= 1; level--) {
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
  virtual ULL NumElements() { return _n; }
  virtual void SetSize(const unsigned long size) { _max_size = size; }
  size_t GetBytesSize() { return _n * (sizeof(K) + sizeof(V)); }

  virtual std::vector<kvPair<K, V>> GetAll() {
    std::vector<kvPair<K, V>> ret = std::vector<kvPair<K, V>>();
    Node *node = p_listHead->_forward[1];
    while (node != p_listTail) {
      kvPair<K, V> kv = {node->key, node->value};
      ret.template emplace_back(kv);
      node = node->_forward[1];
    }
    return ret;
  }

  virtual std::vector<kvPair<K, V>> GetAllInRange(const K &k1, const K &k2) {
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

  int GenNodeLevel() {  return ffs(rand() & ((1 << MAXLEVEL) - 1)) - 1; }
};

#endif // LSMTREE_SKIP_LIST_HPP