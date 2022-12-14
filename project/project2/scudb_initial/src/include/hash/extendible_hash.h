/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly hashmap a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <mutex>
#include <map>
#include <memory>
#include "hash/hash_table.h"
using namespace std;


namespace scudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
    //Bucket : store keys and values
    struct Bucket {
        Bucket(int localDepth) {
            this->localDepth = localDepth;
        };
        int localDepth;
        map<K, V> keyMap;
        mutex latch;
    };
public:
    // constructor
    ExtendibleHash(size_t size);
    // helper function to generate hash addressing
    size_t HashKey(const K &key) const;
    // helper function to get global & local depth
    int GetGlobalDepth() const;
    int GetLocalDepth(int bucket_id) const;
    int GetNumBuckets() const;
    // lookup and modifier
    bool Find(const K &key, V &value) override;
    bool Remove(const K &key) override;
    void Insert(const K &key, const V &value) override;

    int getIdx(const K &key) const;

private:
    // add your own member variables here
    int globalDepth;
    int bucketNum;
    size_t bucketMaxSize;
    vector<shared_ptr<Bucket>> buckets;
    mutable mutex latch;
};






} // namespace scudb
