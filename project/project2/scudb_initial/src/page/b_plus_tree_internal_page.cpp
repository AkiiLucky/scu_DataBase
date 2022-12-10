/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>


#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {

    this->SetMaxSize((PAGE_SIZE- sizeof(BPlusTreeInternalPage))/sizeof(MappingType) - 1);
    this->SetSize(0);
    this->SetPageId(page_id);
    this->SetPageType(IndexPageType::INTERNAL_PAGE);
    this->SetParentPageId(parent_id);

}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
    // replace with your own code
    assert(index >= 0 && index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    assert(index >= 0 && index < GetSize());
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    int pageSize = this->GetSize();
    for (int i = 0; i < pageSize; i++)
    {
        if (value == ValueAt(i)) //find the value in this page, return array index
        {
            return i;
        }
    }
    return -1; //if not find the value in this page, return -1
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    assert(index >= 0 && index < GetSize());
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    int pageSize = this->GetSize();
    assert(pageSize > 1);
    int start = 1, end = pageSize - 1;
    //binary search in array
    while (start <= end) { //find the last key in array <= input
        int mid = (end - start) / 2 + start; //avoid int type overflow
        if (comparator(array[mid].first,key) <= 0) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return array[start - 1].second; //return the value of the last key in array <= input
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value,
                                                     const KeyType &new_key,
                                                     const ValueType &new_value) {
    array[0].second = old_value;
    array[1].first = new_key;
    array[1].second = new_value;
    this->SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value,
                                                    const KeyType &new_key,
                                                    const ValueType &new_value) {
    int indexPosToInsert = ValueIndex(old_value) + 1;
    assert(indexPosToInsert > 0);
    IncreaseSize(1);
    int newSize = GetSize();
    //move back 1 unit all the key and value of index > indexPosToInsert
    for (int i = newSize - 1; i > indexPosToInsert; i--) {
        array[i].first = array[i - 1].first;
        array[i].second = array[i - 1].second;
    }
    //insert new key and new value
    array[indexPosToInsert].first = new_key;
    array[indexPosToInsert].second = new_value;
    return newSize;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
    assert(recipient != nullptr);
    int curSize = GetSize();
    int mid = (curSize)/2;
    //get recipientPageId
    page_id_t recipientPageId = recipient->GetPageId();
    for (int i = mid; i < curSize; i++)
    {
        //move key & value to pairs recipient page
        recipient->array[i - mid].first = array[i].first;
        recipient->array[i - mid].second = array[i].second;

        //then update parent page
        //1.pin children page
        auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
        //2.get children page
        BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
        //3.reset children's parent page is recipientPage
        childTreePage->SetParentPageId(recipientPageId);
        //4.unpin children page and set it dirty
        buffer_pool_manager->UnpinPage(array[i].second,true);
    }
    SetSize(mid); //set current page size is mid
    recipient->SetSize(curSize - mid); //set recipient page size is curSize - mid
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    int curPageSize = GetSize();
    assert(index >= 0 && index < curPageSize);
    for (int i = index + 1; i < curPageSize; i++) {
        //move front 1 unit of i > index
        array[i - 1] = array[i];
    }
    IncreaseSize(-1); //set curPageSize -= 1
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    ValueType res = ValueAt(0);
    IncreaseSize(-1);
    return res;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient,
                                               int index_in_parent,
                                               BufferPoolManager *buffer_pool_manager) {
    /* change parent page */
    //1.pin parent of current page
    Page *parentPage = buffer_pool_manager->FetchPage(this->GetParentPageId());
    //2.get parent page pointer
    BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(parentPage->GetData());
    //3.get key from parent to index 0
    SetKeyAt(0, parent->KeyAt(index_in_parent));
    //4.unpin parent of current page
    buffer_pool_manager->UnpinPage(parent->GetPageId(), false);

    /* change all child page */
    int curPageSize = GetSize();
    int startIdxInRecipient = recipient->GetSize();
    page_id_t recipientPageId = recipient->GetPageId();
    for (int i = 0; i < curPageSize; ++i)
    {
        //move key & value pairs from current page to recipient page
        recipient->array[startIdxInRecipient + i].first = array[i].first;
        recipient->array[startIdxInRecipient + i].second = array[i].second;

        //then update parent page
        //1.pin child page
        Page *childRawPage = buffer_pool_manager->FetchPage(array[i].second);
        //2.get child page pointer
        BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
        //3.reset childPage's parent is recipient page
        childTreePage->SetParentPageId(recipientPageId);
        //4.unpin child page and set child page dirty
        buffer_pool_manager->UnpinPage(array[i].second,true);
    }

    //reset size of recipient page
    recipient->SetSize(startIdxInRecipient + curPageSize);
    //reset size of current page
    this->SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {

}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                                                      BufferPoolManager *buffer_pool_manager) {

    //1.get first pair
    MappingType firstPair(KeyAt(0), ValueAt(0));

    //2.remove first pair
    int currentPageSize = GetSize();
    memmove(array, array + 1, static_cast<size_t>((currentPageSize-1)*sizeof(MappingType)));
    IncreaseSize(-1);

    //3.append firstPair to the tail of recipient page
    recipient->CopyLastFrom(firstPair, buffer_pool_manager);

    //4.update child
    page_id_t childPageId = firstPair.second;
    //4.1 pin child page
    Page *page = buffer_pool_manager->FetchPage(childPageId);
    assert (page != nullptr);
    //4.2 get child page pointer
    BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    //4.3 reset parent page
    child->SetParentPageId(recipient->GetPageId());
    //4.4 unpin child page and set it dirty
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);

    //5.update key & value pair in its parent page.
    //5.1 pin parent page
    page = buffer_pool_manager->FetchPage(GetParentPageId());
    //5.2 get parent page pointer
    B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
    //5.3 reset index key of parent page
    parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
    //5.4 unpin parent page
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair,
                                                  BufferPoolManager *buffer_pool_manager) {
    int pageSize = GetSize();
    assert(pageSize < GetMaxSize());
    array[pageSize] = pair;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                                                       int parent_index,
                                                       BufferPoolManager *buffer_pool_manager) {
    int pageSize = GetSize();
    int lastIdx = pageSize - 1;
    MappingType pair (KeyAt(lastIdx),ValueAt(lastIdx));
    IncreaseSize(-1);
    recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair,
                                                   int parent_index,
                                                   BufferPoolManager *buffer_pool_manager) {

    /* insert first pair */
    int pageSize = GetSize();
    memmove(array + 1, array, pageSize*sizeof(MappingType));
    IncreaseSize(1);
    array[0] = pair;

    /* update parentPageId of child */
    page_id_t childPageId = pair.second;
    //pin child page
    Page *childPage = buffer_pool_manager->FetchPage(childPageId);
    assert (childPage != nullptr);
    //get child page pointer
    BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(childPage->GetData());
    //reset parentPageId
    child->SetParentPageId(GetPageId());
    //unpin child page and set it dirty
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);

    /* update pairs in parent page */
    //pin parent page
    Page *parentPage = buffer_pool_manager->FetchPage(GetParentPageId());
    //get parent page pointer
    B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parentPage->GetData());
    //reset key of parent page
    parent->SetKeyAt(parent_index, array[0].first);
    //unpin parent page and set it dirty
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(std::queue<BPlusTreePage *> *queue,
                                                     BufferPoolManager *buffer_pool_manager) {
    for (int i = 0; i < GetSize(); i++)
    {
        Page *page = buffer_pool_manager->FetchPage(array[i].second);
        if (page == nullptr)
        {
            throw Exception(EXCEPTION_TYPE_INDEX,"all page are pinned while printing");
        }
        BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        queue->push(node);
    }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb


