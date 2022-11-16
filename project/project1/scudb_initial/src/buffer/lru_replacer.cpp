/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
    head = make_shared<Node>(); //head Node
    tail = make_shared<Node>(); //tail Node
    head->next = tail;
    tail->prev = head;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    lock_guard<mutex> lck(latch);
    shared_ptr<Node> curPtr;
    if (hashmap.find(value) != hashmap.end()) //the value is in doubleLinkedList
    {
        //get the ptr of value from doubleLinkedList
        curPtr = hashmap[value];
        shared_ptr<Node> prevPtr = curPtr->prev;
        shared_ptr<Node> nextPtr = curPtr->next;
        //remove the ptr of value from doubleLinkedList
        prevPtr->next = nextPtr;
        nextPtr->prev = prevPtr;
    }
    else  //the value is not in doubleLinkedList
    {
        //create a new ptr of value
        curPtr = make_shared<Node>(value);
    }
    //insert ptr of value into the head of doubleLinkedList
    shared_ptr<Node> firstPtr = head->next;
    firstPtr->prev = curPtr;
    curPtr->next = firstPtr;
    head->next = curPtr;
    curPtr->prev = head;
    //insert the <value, ptr of value> into hashmap
    hashmap[value] = curPtr;
    return;
}

/* If LRU is non-empty, pop the long time not used member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    lock_guard<mutex> lck(latch);
    if (head->next == tail) { //doubleLinkedList is empty and return false
        return false;
    }
    //remove the last Node from doubleLinkedList
    shared_ptr<Node> last = tail->prev;
    value = last->val;
    last->prev->next = tail;
    tail->prev = last->prev;
    //remove the value of last Node from hashmap
    hashmap.erase(last->val);
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    lock_guard<mutex> lck(latch);
    if (hashmap.find(value) != hashmap.end())
    {   //if the value existed in linkedList, remove the value from doubleLinkedList
        shared_ptr<Node> cur = hashmap[value];
        cur->prev->next = cur->next;
        cur->next->prev = cur->prev;
    }
    //remove the value from hashmap and return
    return hashmap.erase(value);
}

template <typename T> size_t LRUReplacer<T>::Size() {
    lock_guard<mutex> lck(latch);
    return hashmap.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb
