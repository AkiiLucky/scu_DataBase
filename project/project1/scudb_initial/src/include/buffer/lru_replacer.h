/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <unordered_map>
#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace scudb {

template <typename T> class LRUReplacer : public Replacer<T> {
    //the Node of doubleLinkedList
    struct Node {
        Node() {};
        Node(T val) : val(val) {};
        T val;
        shared_ptr<Node> prev; //point to prevent node
        shared_ptr<Node> next; //point to next node
    };

public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
    // add your member variables here
    shared_ptr<Node> head; //the head Node of doubleLinkedList
    shared_ptr<Node> tail; //the tail Node of doubleLinkedList
    //use hashmap to store all copied ptr of doubleLinkedList, because hashmap make search more fast
    unordered_map<T,shared_ptr<Node>> hashmap;
    mutable mutex latch;

};

} // namespace scudb
