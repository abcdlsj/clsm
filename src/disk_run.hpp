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

 private:
  long _capacity;
  std::string _filename;
  std::vector<K> _fencePointers;
  int _maxFP;
  int _runID;
  int _level;

  double _bfFalsePositive;  // bloom filter false positive

  void doMunmap() {
    size_t filesize = _capacity * sizeof(KVPair_t);

    if (munmap(map, filesize) == -1) {
      perror("Error un-mmapping the file");
    }

    close(fd);
    fd = -2;  // 设置成 -2，和错误的 -1 区分开
  }

 public:
  typedef kvPair<K, V> KVPair_t;
  KVPair_t *map;
  int fd;
  int _blockSize;
  BloomFilter<K> bf;

  K minKey = INT_MIN, maxKey = INT_MAX;

  DiskRun<K, V>(long capacity, int blockSize, int level, int runID,
                double bfFalsePositive)
      : _capacity(capacity),
        _level(level),
        _maxFP(0),
        _bfFalsePositive(bfFalsePositive),
        _blockSize(blockSize),
        bf(capacity, bfFalsePositive) {
    _filename =
        "C_" + std::to_string(level) + "_" + std::to_string(runID) + ".clsm";

    size_t filesize = capacity * sizeof(KVPair_t);

    long long ret;

    fd = open(_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);

    if (fd == -1) {
      perror("Error opening file for writing");
      exit(EXIT_FAILURE);
    }

    ret = lseek(fd, filesize - 1, SEEK_SET);
    if (ret == -1) {
      close(fd);
      perror("Error calling lseek() to 'stretch' the file");
      exit(EXIT_FAILURE);
    }

    ret = write(fd, "", 1);
    if (ret != 1) {
      close(fd);
      perror("Error writing last byte of the file");
      exit(EXIT_FAILURE);
    }

    map = (KVPair_t *)mmap(0, filesize, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                           0);
    if (map == MAP_FAILED) {
      close(fd);
      perror("Error in mmapping the file");
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

  void setCapacity(const long newCapacity) { _capacity = newCapacity; }

  long getCapacity() { return _capacity; }

  void writeData(const KVPair_t *run, const size_t offset, const long len) {
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

  long binarySearch(const long offset, const long n, const K &key,
                    bool &isFound) {
    if (n == 0) {
      isFound = true;
      return offset;
    }
    long le = offset, ri = offset + n - 1, mid;
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

  void getFencePointers(const K &key, long &start, long &end) {
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
      int le = 0, ri = _maxFP, mid;
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

  long getIndex(const K &key, bool &isFound) {
    long start, end;
    getFencePointers(key, start, end);
    long ret = binarySearch(start, end - start, key, isFound);
    return ret;
  }

  V search(const K &key, bool &isFound) {
    long idx = getIndex(key, isFound);
    V ret = map[idx].value;
    return isFound ? ret : static_cast<V>(NULL);
  }

  void getRangeIndex(const K &k1, const K &k2, long &idx1, long idx2) {
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
};

#endif  // LSMTREE_DISK_RUN_HPP