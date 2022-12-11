/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage, int indexInPage, BufferPoolManager *bufferPoolManager);

  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  INDEXITERATOR_TYPE &operator++();

private:
    // add your own private member variables here
    void UnlockAndUnPin() ;

    int indexInPage_;
    B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage_;
    BufferPoolManager *bufferPoolManager_;
};

} // namespace scudb

