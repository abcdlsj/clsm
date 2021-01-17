#ifndef LSMTREE_DISK_LEVEL_HPP
#define LSMTREE_DISK_LEVEL_HPP

#include <assert.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "disk_run.hpp"
#include "run.hpp"

#define LEFTCHILD(x) 2 * x + 1
#define RIGHTCHILD(x) 2 * x + 2
#define PARENT(x) (x - 1) / 2

int STONE = INT_MIN;

template <class K, class V>
class DiskLevel {
 public:
  typedef kvPair<K, V> KVPair_t;
  typedef std::pair<kvPair<K, V>, int> KVIntPair_t;
  KVPair_t KVPMAX;
  KVIntPair_t KVPINTMAX;
  V V_STONE = static_cast<V>(STONE);

  struct StaticHead {
    int size;
    std::vector<KVIntPair_t> arr;
    KVIntPair_t max;
    StaticHead(unsigned int sz, KVIntPair_t mx) {
      size = 0;
      arr = std::vector<KVIntPair_t>(sz, mx);
      max = mx;
    }

    void push(KVIntPair_t blob) {
      unsigned int idx = size++;
      while (idx && blob < arr[PARENT(idx)]) {
        arr[idx] = arr[PARENT(idx)];
        idx = PARENT(idx);
      }
      arr[idx] = blob;
    }

    void heapify(int idx) {
      int smallestIdx =
          (LEFTCHILD(idx) < size && arr[LEFTCHILD(idx)] < arr[idx])
              ? LEFTCHILD(idx)
              : idx;
      if (RIGHTCHILD(idx) < size && arr[RIGHTCHILD(idx)] < arr[smallestIdx]) {
        smallestIdx = RIGHTCHILD(idx);
      }

      if (smallestIdx != idx) {
        KVIntPair_t tmp = arr[idx];
        arr[idx] = arr[smallestIdx];
        arr[smallestIdx] = tmp;
        heapify(smallestIdx);
      }
    }

    KVIntPair_t pop() {
      KVIntPair_t ret = arr[0];
      arr[0] = arr[--size];
      heapify(0);
      return ret;
    }
  };

  int _level;
  unsigned int _blockSize;     // number of elements per fence pointer;
  unsigned int _numRuns;       // number of runs in a level;
  unsigned int _activeRunIdx;  // index of active run;
  unsigned int _mergeSize;     // # of runs to merge downloads;
  UL _runSize;                 // number of elements in a run;
  double _bfFalsePositive;

  std::vector<DiskRun<K, V> *> runs;

  DiskLevel<K, V>(unsigned int blockSize, int level, UL runSize,
                  unsigned int numRuns, unsigned int mergeSize,
                  double bfFalsePositive)
      : _blockSize(blockSize),
        _level(level),
        _runSize(runSize),
        _numRuns(numRuns),
        _mergeSize(mergeSize),
        _bfFalsePositive(bfFalsePositive) {
    KVPMAX = KVPair_t{INT_MAX, 0};
    KVPINTMAX = KVIntPair_t(KVPMAX, -1);
    for (auto i = 0; i < _numRuns; i++) {
      DiskRun<K, V> *run =
          new DiskRun<K, V>(_runSize, _blockSize, _level, i, _bfFalsePositive);
      runs.push_back(run);
    }
  }

  ~DiskLevel<K, V>() {
    for (auto i = 0; i < runs.size(); i++) {
      delete runs[i];
    }
  }

  void addRuns(std::vector<DiskRun<K, V> *> &runList, const UL runlen,
               bool isLastLevel) {
    StaticHead h = StaticHead(static_cast<int>(runlen), KVPINTMAX);

    std::vector<int> heads(runList.size(), 0);
    for (auto i = 0; i < runList.size(); i++) {
      KVPair_t kvp = runList[i]->map[0];
      h.push(KVIntPair_t(kvp, i));
    }

    int j = -1;
    K lastKey = INT_MAX;
    unsigned int lastK = INT_MIN;
    while (h.size != 0) {
      auto val_run_pair = h.pop();
      if (lastKey == val_run_pair.first.key) {
        if (lastK < val_run_pair.second) {
          runs[_activeRunIdx]->map[j] = val_run_pair.first;
        }
      } else {
        ++j;
        if (j != -1 && isLastLevel &&
            runs[_activeRunIdx]->map[j].value == V_STONE) {
          --j;
        }
        runs[_activeRunIdx]->map[j] = val_run_pair.first;
      }
      lastKey = val_run_pair.first.key;
      lastK = val_run_pair.second;

      unsigned int k = val_run_pair.second;
      if (++heads[k] < runList[k]->getCapacity()) {
        KVPair_t kvp = runList[k]->map[heads[k]];
        h.push(KVIntPair_t(kvp, k));
      }
    }

    if (isLastLevel && runs[_activeRunIdx]->map[j].value == V_STONE) {
      --j;
    }

    runs[_activeRunIdx]->setCapacity(j + 1);
    runs[_activeRunIdx]->constructIndex();

    if (j + 1 > 0) {
      ++_activeRunIdx;
    }
  }

  void addRunByArray(KVPair_t *runToAdd, const UL runlen) {
    assert(_activeRunIdx < _numRuns);
    assert(runlen == _runSize);
    runs[_activeRunIdx]->writeData(runToAdd, 0, runlen);
    runs[_activeRunIdx]->constructIndex();
    _activeRunIdx++;
  }

  std::vector<DiskRun<K, V> *> getRunsToMerge() {
    std::vector<DiskRun<K, V> *> toMerge;
    for (auto i = 0; i < _mergeSize; i++) {
      toMerge.push_back(runs[i]);
    }

    return toMerge;
  }

  void freeMergedRuns(std::vector<DiskRun<K, V> *> &toFree) {
    assert(toFree.size() == _mergeSize);
    for (auto i = 0; i < _mergeSize; i++) {
      assert(toFree[i]->_level == _level);
      delete toFree[i];
    }

    runs.erase(runs.begin(), runs.begin() + _mergeSize);
    _activeRunIdx -= _mergeSize;
    for (auto i = 0; i < _activeRunIdx; i++) {
      runs[i]->_runID = i;
      std::string newName = "C_" + std::to_string(runs[i]->_level) + "_" +
                            std::to_string(runs[i]->_runID) + ".clsm";

      if (rename(runs[i]->_filename.c_str(), newName.c_str())) {
        perror(("Error renaming file " + runs[i]->_filename + " to " + newName)
                   .c_str());
        exit(EXIT_FAILURE);
      }
      runs[i]->_filename = newName;
    }

    for (auto i = _activeRunIdx; i < _numRuns; i++) {
      DiskRun<K, V> *newRun =
          new DiskRun<K, V>(_runSize, _blockSize, _level, i, _bfFalsePositive);
      runs.push_back(newRun);
    }
  }

  bool isLevelFull() { return _activeRunIdx == _numRuns; }

  bool isLevelEmpty() { return _activeRunIdx == 0; }

  V search(const K &key, bool &isFound) {
    int maxRunToSearch = _activeRunIdx - 1;
    for (auto i = maxRunToSearch; i >= 0; i--) {
      if (runs[i]->maxKey == INT_MIN || key < runs[i]->minKey ||
          key > runs[i]->maxKey || !runs[i]->bf.isContained(&key, sizeof(key))) {
        continue;
      }

      V lookupRet = runs[i]->search(key, isFound);
      if (isFound) {
        return lookupRet;
      }
    }

    return static_cast<V>(NULL);
  }

  UL eltsNums() {
    UL sum = 0;
    for (auto i = 0; i < _activeRunIdx; i++) sum += runs[i]->getCapacity();
    return sum;
  }
};

#endif  // LSMTREE_DISK_LEVEL_HPP
