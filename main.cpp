#include <ctime>
#include <iostream>
#include <random>

#include "src/bloom_filter.hpp"
#include "src/hash_map.hpp"
#include "src/lsm.hpp"
#include "src/skip_list.hpp"

using namespace std;

struct timespec start, finish;

void bloomFilterTest() {
  std::random_device rand_dev;
  std::mt19937 generator(rand_dev());
  std::uniform_int_distribution<int> distribution(INT32_MIN, INT32_MAX);

  const int num_inserts = 1000;
  double fprate = .001;
  BloomFilter<int32_t> bf = BloomFilter<int32_t>(num_inserts, fprate);

  std::vector<int> to_insert;
  for (int i = 0; i < num_inserts; i++) {
    int insert = distribution(generator);
    to_insert.push_back(insert);
  }
  clock_gettime(CLOCK_MONOTONIC, &start);
  std::cout << "Starting inserts" << std::endl;
  for (int i = 0; i < num_inserts; i++) {
    bf.add(&i, sizeof(i));
  }
  clock_gettime(CLOCK_MONOTONIC, &finish);
  double total_insert = (finish.tv_sec - start.tv_sec);
  total_insert += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

  std::cout << "Time: " << total_insert << " s" << std::endl;
  std::cout << "Inserts per second: " << (int)num_inserts / total_insert << " s"
            << std::endl;
  int fp = 0;
  for (int i = num_inserts; i < 2 * num_inserts; i++) {
    bool lookup = bf.isContained(&i, sizeof(i));
    if (lookup) {
      // cout << i << "found but didn't exist" << endl;
      fp++;
    }
  }
  cout << fp << endl;
  cout << "FP rate: " << ((double)fp / double(num_inserts)) << endl;
}

int main() {}
