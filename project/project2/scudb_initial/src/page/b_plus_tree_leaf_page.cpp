/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {

        this->SetPageType(IndexPageType::LEAF_PAGE);
        this->SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage))/sizeof(MappingType) - 1);
        this->SetSize(0);
//        assert(sizeof(BPlusTreeLeafPage) == 28);
        this->SetPageId(page_id);
        this->SetParentPageId(parent_id);
        this->SetNextPageId(INVALID_PAGE_ID);

    }

/**
 * Helper methods to set/get next page id
 */
    INDEX_TEMPLATE_ARGUMENTS
    page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
        return next_page_id_;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
        next_page_id_ = next_page_id;
    }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
        int start = 0, end = GetSize() - 1; //binary search
        while (start <= end) { //find the last key in array <= input
            int mid = (end - start) / 2 + start;
            if (comparator(array[mid].first,key) >= 0) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        return end + 1;
    }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
        assert(index >= 0 && index < GetSize());
        return array[index].first;
    }

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
        assert(index >= 0 && index < GetSize());
        return array[index];
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                           const ValueType &value,
                                           const KeyComparator &comparator) {
        int idxToInsert = KeyIndex(key,comparator);
        assert(idxToInsert >= 0);
        IncreaseSize(1);
        int curPageSize = this->GetSize();
        for (int i = curPageSize - 1; i > idxToInsert; i--)
        {
            //move back 1 unit of index > idxToInsert
            array[i].first = array[i - 1].first;
            array[i].second = array[i - 1].second;
        }
        array[idxToInsert].first = key;
        array[idxToInsert].second = value;
        return curPageSize;
    }

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,
                                                __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
        assert(recipient != nullptr);

        //1.copy last half key & value pairs
        int totalSize = GetMaxSize() + 1;
        int idxToCopy = (totalSize) / 2;
        for (int i = idxToCopy; i < totalSize; i++)
        {
            //move key & value pairs to recipient page
            recipient->array[i - idxToCopy].first = array[i].first;
            recipient->array[i - idxToCopy].second = array[i].second;
        }

        //2.1 reset next page pointer of recipient page
        recipient->SetNextPageId(this->GetNextPageId());
        //2.2 reset next page pointer of current page
        this->SetNextPageId(recipient->GetPageId());

        //3.reset current page size
        SetSize(idxToCopy);

        //4.reset recipient page size
        recipient->SetSize(totalSize - idxToCopy);

    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                            const KeyComparator &comparator) const {
        int idxToFind = KeyIndex(key,comparator);
        int pageSize = GetSize();
        if (idxToFind >= 0 && idxToFind < pageSize)
        {
            if (comparator(array[idxToFind].first, key) == 0)
            {
                //find the key, reset value and return true
                value = array[idxToFind].second;
                return true;
            } else {
                return false;
            }
        }
        return false; //not find the key, return false
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
        int firstIdxLargerEqualThanKey = KeyIndex(key,comparator);
        int pageSize = GetSize();
        if (firstIdxLargerEqualThanKey >= pageSize) {
            return pageSize;
        }
        if (comparator(key,KeyAt(firstIdxLargerEqualThanKey)) != 0) {
            return pageSize;
        }
        int targetIdx = firstIdxLargerEqualThanKey;
        memmove(array+targetIdx, array+targetIdx+1,static_cast<size_t>((pageSize-targetIdx-1)*sizeof(MappingType)));
        IncreaseSize(-1);
        return pageSize-1;
    }

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                               int, BufferPoolManager *) {
        assert(recipient != nullptr);
        int curPageSize = GetSize();
        int startIdxToMove = recipient->GetSize();
        for (int i = 0; i < curPageSize; i++)
        {
            //move all key & value pairs to the tail of recipient page
            recipient->array[startIdxToMove + i].first = array[i].first;
            recipient->array[startIdxToMove + i].second = array[i].second;
        }
        //reset next page pointer of recipient page
        recipient->SetNextPageId(GetNextPageId());
        //reset size of recipient page
        recipient->IncreaseSize(curPageSize);
        //reset size of current page
        this->SetSize(0);

    }
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                                                      BufferPoolManager *buffer_pool_manager) {
        //1.get first pair
        MappingType pair = GetItem(0);

        //2.remove first pair
        memmove(array, array + 1, static_cast<size_t>(GetSize()*sizeof(MappingType)));

        //3.update page size
        IncreaseSize(-1);

        //4.append firstPair to the tail of recipient page
        recipient->CopyLastFrom(pair);

        //5.update key & value pair in its parent page.
        //5.1 pin parent page
        Page *parentPage = buffer_pool_manager->FetchPage(GetParentPageId());
        //5.2 get parent page pointer
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parentPage->GetData());
        //5.3 reset index key of parent page
        parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
        //5.4 unpin parent page and set it dirty
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
        int pageSize = GetSize();
        assert(pageSize < GetMaxSize());
        array[pageSize] = item;
        IncreaseSize(1);
    }
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,
                                                       int parentIndex,
                                                       BufferPoolManager *buffer_pool_manager) {
        MappingType lastPair = GetItem(GetSize() - 1);
        IncreaseSize(-1);
        recipient->CopyFirstFrom(lastPair, parentIndex, buffer_pool_manager);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item,
                                                   int parentIndex,
                                                   BufferPoolManager *buffer_pool_manager) {
        //insert item to first index
        int pageSize = GetSize();
        memmove(array + 1, array, pageSize*sizeof(MappingType));
        array[0] = item;
        IncreaseSize(1);

        //update key & value pair in its parent page.
        //pin parent page
        Page *parentPage = buffer_pool_manager->FetchPage(GetParentPageId());
        //get parent page pointer
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parentPage->GetData());
        //reset index key of parent page
        parent->SetKeyAt(parentIndex, array[0].first);
        //unpin parent page and set it dirty
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
    INDEX_TEMPLATE_ARGUMENTS
    std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
        if (GetSize() == 0) {
            return "";
        }
        std::ostringstream stream;
        if (verbose) {
            stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
                   << "]<" << GetSize() << "> ";
        }
        int entry = 0;
        int end = GetSize();
        bool first = true;

        while (entry < end) {
            if (first) {
                first = false;
            } else {
                stream << " ";
            }
            stream << std::dec << array[entry].first;
            if (verbose) {
                stream << "(" << array[entry].second << ")";
            }
            ++entry;
        }
        return stream.str();
    }

    template class BPlusTreeLeafPage<GenericKey<4>, RID,
            GenericComparator<4>>;
    template class BPlusTreeLeafPage<GenericKey<8>, RID,
            GenericComparator<8>>;
    template class BPlusTreeLeafPage<GenericKey<16>, RID,
            GenericComparator<16>>;
    template class BPlusTreeLeafPage<GenericKey<32>, RID,
            GenericComparator<32>>;
    template class BPlusTreeLeafPage<GenericKey<64>, RID,
            GenericComparator<64>>;
} // namespace scudb
