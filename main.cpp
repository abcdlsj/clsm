#include <iostream>

#include "src/skip_list.hpp"

int main() {
  auto *skp = new SkipList<int, int>(1, 100);
  for (int i = 0; i < 100; i++) {
    skp->InsertKey(i, i *100);
  }

  for (int i = 0; i < 50; i++) {
    skp->DeleteKey(i);
  }
  auto vec = skp->GetAll();
  for (auto &e : vec) {
    std::cout << e.key << " " << e.value << std::endl;
  }
}