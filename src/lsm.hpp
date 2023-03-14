#ifndef LSMTREE_LSM_HPP
#define LSMTREE_LSM_HPP

#include <algorithm>
#include <mutex>
#include <thread>

#include "bloom_filter.hpp"
#include "disk_level.hpp"
#include "hash_map.hpp"
#include "run.hpp"
#include "skip_list.hpp"

template <class K, class V>
class LSM {
  typedef SkipList<K, V> RunType;

  long _eltsPerRun;
  long _n;

  double _fracRunsMerged; // 合并的倍数，(0, 1]
  double _bfFalsePositive; // 假阳性的概率

  int _activeRunIdx;
  int _numRuns; // 内存最大 run 数目
  int _numDiskLevels;
  int _diskRunsPerLevel; // 每层 level 数
  int _numToMerge; // C_0 一次 Merge 的 runs 数
  int _blockSize;

  std::thread mergeThread;

 public:
  V V_TOMBSTONE = static_cast<V>(TOMBSTONE);
  std::mutex *mergeLock;
  std::vector<Run<K, V> *> C_0;
  std::vector<BloomFilter<K> *> filters;
  std::vector<DiskLevel<K, V> *> diskLevels;
  LSM<K, V>(const LSM<K, V> &other) = default;
  LSM<K, V>(LSM<K, V> &&other) = default;

  LSM<K, V>(long eltsPerRun, int numRuns, double fracMerged,
            double bfFalsePositive, int blockSize, int diskRunsPerLevel)
      : _eltsPerRun(eltsPerRun),
        _numRuns(numRuns),
        _fracRunsMerged(fracMerged),
        _diskRunsPerLevel(diskRunsPerLevel),
        _numToMerge(ceil(_fracRunsMerged * _numRuns)),
        _blockSize(blockSize),
        _bfFalsePositive(bfFalsePositive),
        _activeRunIdx(0),
        _n(0) {
    DiskLevel<K, V> *diskLevel = new DiskLevel<K, V>(
        blockSize, 1, _numToMerge * _eltsPerRun, _diskRunsPerLevel,
        ceil(_diskRunsPerLevel * _fracRunsMerged), _bfFalsePositive);

    diskLevels.push_back(diskLevel);
    _numDiskLevels = 1;

    for (auto i = 0; i < _numRuns; i++) {
      RunType *run = new RunType(INT32_MIN, INT32_MAX);
      run->setSize(_eltsPerRun);
      C_0.push_back(run);

      BloomFilter<K> *bf = new BloomFilter<K>(_eltsPerRun, _bfFalsePositive);
      filters.push_back(bf);
    }

    mergeLock = new std::mutex();
  }

  ~LSM<K, V>() {
    if (mergeThread.joinable()) {
      mergeThread.join();
    }
    delete mergeLock;
    for (auto i = 0; i < C_0.size(); i++) {
      delete C_0[i];
      delete filters[i];
    }

    for (auto i = 0; i < diskLevels.size(); i++) {
      delete diskLevels[i];
    }
  }

  void insertKey(K &key, V &value) {
    if (C_0[_activeRunIdx]->eltsNums() >= _eltsPerRun) {
      ++_activeRunIdx;
    }

    if (_activeRunIdx >= _numRuns) {
      doMerge();
    }

    C_0[_activeRunIdx]->insertKey(key, value);
  }

  bool search(K &key, V &value) {
    bool isFound = false;
    for (int i = _activeRunIdx; i >= 0; i--) {
      if (key < C_0[i]->getMin() || key > C_0[i]->getMax() ||
          !filters[i]->isContain(&key, sizeof(K))) {
        continue;
      }

      value = C_0[i]->search(key, isFound);
      if (isFound) {
        return value != V_TOMBSTONE;
      }
    }

    if (mergeThread.joinable()) {
      mergeThread.join();
    }

    for (auto i = 0; i < _numDiskLevels; i++) {
      value = diskLevels[i]->search(key, isFound);
      if (isFound) {
        return value != V_TOMBSTONE;
      }
    }

    return false;
  }

  void deleteKey(K &key) { insertKey(key, V_TOMBSTONE); }

  std::vector<kvPair<K, V>> range(K &k1, K &k2) {
    if (k2 <= k1) {
      return std::vector<kvPair<K, V>>{};
    }

    auto hashtable = HashTable<K, V>(4096 * 1000);
    std::vector<kvPair<K, V>> elts_in_range = std::vector<kvPair<K, V>>();

    for (int i = _activeRunIdx; i >= 0; i--) {
      std::vector<kvPair<K, V>> cur_elts = C_0[i]->getAllInRange(k1, k2);
      if (cur_elts.size() != 0) {
        elts_in_range.reserve(elts_in_range.size() + cur_elts.size());

        for (auto j = 0; j < cur_elts.size(); j++) {
          V dummy = hashtable.putIfEmpty(cur_elts[j].key, cur_elts[j].value);
          if (!dummy && cur_elts[j].value != V_TOMBSTONE) {
            elts_in_range.push_back(cur_elts[j]);
          }
        }
      }
    }

    if (mergeThread.joinable()) {
      mergeThread.join();
    }

    for (auto i = 0; i < _numDiskLevels; i++) {
      for (auto j = diskLevels[i]->_activeRunIdx - 1; j >= 0; j--) {
        long i1, i2;
        diskLevels[i]->runs[j]->getRangeIndex(k1, k2, i1, i2);

        if (i2 - i1 != 0) {
          auto oldSize = elts_in_range.size();
          elts_in_range.reserve(oldSize + (i2 - i1));
          for (long k = i1; k < i2; k++) {
            auto kv = diskLevels[i]->runs[j]->map[k];
            V dummy = hashtable.putIfEmpty(kv.key, kv.value);
            if (!dummy && kv.value != V_TOMBSTONE) {
              elts_in_range.push_back(kv);
            }
          }
        }
      }
    }

    return elts_in_range;
  }

  void printElts() {
    if (mergeThread.joinable()) mergeThread.join();
    std::cout << "MEMORY BUFFER:\n";
    for (auto i = 0; i < _activeRunIdx; i++) {
      std::cout << "MEMORY BUFFER RUN: " << i << std::endl;
      auto all = C_0[i]->getAll();
      for (auto &c : all) {
        std::cout << c.key << ":" << c.value << " ";
      }
      std::cout << std::endl;
    }

    std::cout << "DISK BUFFER:\n";
    for (auto i = 0; i < _numDiskLevels; i++) {
      std::cout << "DISK LEVEL: " << i << std::endl;
      for (auto j = 0; j < diskLevels[i]->_activeRunIdx; j++) {
        std::cout << "RUN: " << j << std::endl;
        for (auto k = 0; k < diskLevels[i]->runs[j]->getCapacity(); k++) {
          std::cout << diskLevels[i]->runs[j]->map[k].key << ":"
                    << diskLevels[i]->runs[j]->map[k].value << " ";
        }
        std::cout << std::endl;
      }

      std::cout << std::endl;
    }
  }

  void printStats() {
    std::cout << "Number of Elements: " << size() << std::endl;
    std::cout << "Number of Elements in Buffer (including deletes): "
              << bufferNums() << std::endl;

    for (int i = 0; i < diskLevels.size(); i++) {
      std::cout << "Number of Elements in Disk Level: " << i
                << "(including deletes): " << diskLevels[i]->eltsNums()
                << std::endl;
    }
    std::cout << "KEY VALUE DUMP BY LEVEL" << std::endl;
    printElts();
  }

  // 从 disk[level - 1] 中拿到 runs add 到当前 level 
  void mergeRunsToLevel(int level) {
    bool isLastLevel = false;

    if (level == _numDiskLevels) {
      DiskLevel<K, V> *newLevel = new DiskLevel<K, V>(
          _blockSize, level + 1,
          diskLevels[level - 1]->_runSize * diskLevels[level - 1]->_mergeSize,
          _diskRunsPerLevel, ceil(_diskRunsPerLevel * _fracRunsMerged),
          _bfFalsePositive);
      diskLevels.push_back(newLevel);
      _numDiskLevels++;
    }

    if (diskLevels[level]->isLevelFull()) {
      mergeRunsToLevel(level + 1);
    }

    if (level + 1 == _numDiskLevels && diskLevels[level]->isLevelEmpty()) {
      isLastLevel = true;
    }

    // 从 disklevel 中得到用于 merge 的 runs [0, _mergeSize)
    std::vector<DiskRun<K, V> *> runs_to_merge =
        diskLevels[level - 1]->getRunsToMerge();
    long runLen = diskLevels[level - 1]->_runSize;
    diskLevels[level]->addRuns(runs_to_merge, runLen, isLastLevel);
    diskLevels[level - 1]->freeMergedRuns(runs_to_merge);
  }

  // merge 的主函数，把 runs merge 到磁盘的最浅层级当中
  void mergeRuns(std::vector<Run<K, V> *> runs_to_merge,
                 std::vector<BloomFilter<K> *> bf_to_merge) {
    std::vector<kvPair<K, V>> to_merge = std::vector<kvPair<K, V>>();
    to_merge.reserve(_eltsPerRun * _numToMerge);
    for (auto i = 0; i < runs_to_merge.size(); i++) {
      auto all = (runs_to_merge)[i]->getAll();

      to_merge.insert(to_merge.begin(), all.begin(), all.end());
      delete (runs_to_merge)[i];
      delete (bf_to_merge)[i];
    }

    sort(to_merge.begin(), to_merge.end());
    mergeLock->lock();
    if (diskLevels[0]->isLevelFull()) {
      mergeRunsToLevel(1);
    }
    diskLevels[0]->addRunByArray(&to_merge[0], to_merge.size());
    mergeLock->unlock();
  }

  // 从 memory 向 disk merge
  // mergeruns 是 C_0 [0, _numToMerge)
  void doMerge() {
    if (_numToMerge == 0) return;
    std::vector<Run<K, V> *> runs_to_merge = std::vector<Run<K, V> *>();
    std::vector<BloomFilter<K> *> bf_to_merge = std::vector<BloomFilter<K> *>();
    for (auto i = 0; i < _numToMerge; i++) {
      runs_to_merge.push_back(C_0[i]);
      bf_to_merge.push_back(filters[i]);
    }

    if (mergeThread.joinable()) {
      mergeThread.join();
    }

    mergeThread =
        std::thread(&LSM::mergeRuns, this, runs_to_merge, bf_to_merge);

    C_0.erase(C_0.begin(), C_0.begin() + _numToMerge);
    filters.erase(filters.begin(), filters.begin() + _numToMerge);

    _activeRunIdx -= _numToMerge;
    for (auto i = _activeRunIdx; i < _numRuns; i++) {
      RunType *run = new RunType(INT32_MIN, INT32_MAX);
      run->setSize(_eltsPerRun);
      C_0.push_back(run);

      BloomFilter<K> *bf = new BloomFilter<K>(_eltsPerRun, _bfFalsePositive);
      filters.push_back(bf);
    }
  }

  long bufferNums() {
    if (mergeThread.joinable()) mergeThread.join();
    long sum = 0;
    for (auto i = 0; i <= _activeRunIdx; i++) sum += C_0[i]->eltsNums();
    return sum;
  }

  long size() {
    K min = INT_MIN, max = INT_MAX;
    auto r = range(min, max);
    return r.size();
  }
};

#endif  // LSMTREE_LSM_HPP
