#ifndef LSMTREE_DISK_RUN_HPP
#define LSMTREE_DISK_RUN_HPP

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>

#include "bloom_filter.hpp"
#include "climits"
#include "run.hpp"

template <class K, class V>
class DiskLevel;
template <class K, class V>
class DiskRun {
  friend class DiskLevel<K, V>;

 public:
  typedef kvPair<K, V> KVPair_t;
  KVPair_t *map;
  int fd;
  unsigned int _blockSize;
  BloomFilter<K> bf;

  K minKey = INT_MIN, maxKey = INT_MAX;

  DiskRun<K, V>(UL capacity, unsigned int blockSize, int level, int runID,
                double bfFalsePositive)
      : _capacity(capacity),
        _level(level),
        _maxFP(0),
        _blockSize(blockSize),
        _bfFalsePositive(bfFalsePositive),
        bf(capacity, bfFalsePositive) {
    _filename =
        "c_" + std::to_string(level) + "_" + std::to_string(runID) + ".clsm";

    size_t filesize = capacity * sizeof(KVPair_t);

    LL result;

    fd = open(_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);

    if (fd == -1) {
      perror("Error opening file for writing");
      exit(EXIT_FAILURE);
    }

    result = lseek(fd, filesize - 1, SEEK_SET);
    if (result == -1) {
      close(fd);
      perror("Error calling lseek() to 'stretch' the file");
      exit(EXIT_FAILURE);
    }

    result = write(fd, "", 1);
    if (result != 1) {
      close(fd);
      perror("Error writing last byte of the file");
      exit(EXIT_FAILURE);
    }

    map = (KVPair_t *)mmap(0, filesize, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                           0);
    if (map == MAP_FAILED) {
      close(fd);
      perror("Error mmapping the file");
      exit(EXIT_FAILURE);
    }
  }

  ~DiskRun<K, V>() {
    fsync(fd);
    doMunmap();

    if (remove(_filename.c_str())) {
      perror(("Error removing file " + std::string(_filename)).c_str());
      exit(EXIT_FAILURE);
    }
  }

  void setCapacity(const UL newCapacity) { _capacity = newCapacity; }

  auto getCapacity() { return _capacity; }

  void writeData(const KVPair_t *run, const size_t offset, const UL len) {
    memcpy(map + offset, run, len * sizeof(KVPair_t));
    _capacity = len;
  }

  void constructIndex() {
    _fencePointers.reserve(_capacity / _blockSize);
    _maxFP = -1;

    for (auto i = 0; i < _capacity; i++) {
      bf.add((K *)&map[i].key, sizeof(K));
      if (i % _blockSize == 0) {
        _fencePointers.push_back(map[i].key);
        _maxFP++;
      }
    }

    if (_maxFP >= 0) {
      _fencePointers.resize(_maxFP + 1);
    }

    minKey = map[0].key;
    maxKey = map[_capacity - 1].key;
  }

  UL binarySearch(const UL offset, const UL n, const K &key, bool &isFound) {
    if (n == 0) {
      isFound = true;
      return offset;
    }
    UL le = offset, ri = offset + n - 1, mid;
    while (le < ri) {
      mid = le + (ri - le) / 2;
      if (key > map[mid].key) {
        le = mid + 1;
      } else if (key < map[mid].key) {
        ri = mid - 1;
      } else {
        isFound = true;
        return mid;
      }
    }
    return le;
  }

  void getFencePointers(const K &key, UL &start, UL &end) {
    if (_maxFP == 0) {
      start = 0;
      end = _capacity;
    } else if (key < _fencePointers[1]) {
      start = 0;
      end = _blockSize;
    } else if (key >= _fencePointers[_maxFP]) {
      start = _blockSize * _maxFP;
      end = _capacity;
    } else {
      unsigned int le = 0, ri = _maxFP, mid;
      while (le < ri) {
        mid = le + (ri - le) / 2;
        if (key > _fencePointers[mid]) {
          if (key < _fencePointers[mid + 1]) {
            start = mid * _blockSize;
            end = start + _blockSize;
            return;
          }
          le = mid + 1;
        } else if (key < _fencePointers[mid]) {
          if (key >= _fencePointers[mid - 1]) {
            start = (mid - 1) * _blockSize;
            end = start + _blockSize;
            return;
          }
          ri = mid - 1;
        } else {
          start = mid * _blockSize;
          end = start;
          return;
        }
      }
    }
  }

  UL getIndex(const K &key, bool &isFound) {
    UL start, end;
    getFencePointers(key, start, end);
    UL ret = binarySearch(start, end - start, key, isFound);
    return ret;
  }

  V search(const K &key, bool &isFound) {
    UL idx = getIndex(key, isFound);
    V ret = map[idx].value;
    return isFound ? ret : static_cast<V>(NULL);
  }

  void getRangeIndex(const K &k1, const K &k2, UL &idx1, UL idx2) {
    idx1 = 0, idx2 = 0;

    if (k1 > maxKey || k2 < minKey) {
      return;
    }
    if (k1 >= minKey) {
      bool isFound = false;
      idx1 = getIndex(k1, isFound);
    }
    if (k2 > maxKey) {
      idx2 = _capacity;
      return;
    } else {
      bool isFound = false;
      idx2 = getIndex(k2, isFound);
    }
  }

  void printAll() {
    for (auto i = 0; i < _capacity; i++) std::cout << map[i].key << " ";
    std::cout << std::endl;
  }

 private:
  UL _capacity;
  std::string _filename;
  int _level;
  std::vector<K> _fencePointers;
  unsigned int _maxFP;
  unsigned int _runID;
  double _bfFalsePositive;  // bloom filter false positive

  void doMunmap() {
    size_t filesize = _capacity * sizeof(KVPair_t);

    if (munmap(map, filesize) == -1) {
      perror("Error un-mmapping the file");
    }

    close(fd);
    fd = -2;  // 设置成 -2，和错误的 -1 去分开
  }
};

#endif  // LSMTREE_DISK_RUN_HPP