/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace scudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage, int indexInPage, BufferPoolManager *bufferPoolManager)
            : indexInPage_(indexInPage),leafPage_(leafPage), bufferPoolManager_(bufferPoolManager){}


    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE::~IndexIterator() {
        if (leafPage_ != nullptr) UnlockAndUnPin(); //unpin this leaf page
    }


    INDEX_TEMPLATE_ARGUMENTS
    bool INDEXITERATOR_TYPE::isEnd() {
        return (leafPage_ == nullptr);
    }


    INDEX_TEMPLATE_ARGUMENTS
    const MappingType& INDEXITERATOR_TYPE::operator*() {
        return leafPage_->GetItem(indexInPage_);
    }


    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
        indexInPage_++;
        int pageSize = leafPage_->GetSize();
        if (indexInPage_ >= pageSize) //if next key & value pair is in next page
        {
            page_id_t nextPageId = leafPage_->GetNextPageId();
            //unlock and unpin this page
            UnlockAndUnPin();
            if (nextPageId == INVALID_PAGE_ID) leafPage_ = nullptr; //this page is last page
            else
            {
                //fetch next page from buffer pool
                Page *nextPage = bufferPoolManager_->FetchPage(nextPageId);
                //get reading lock from next page
                nextPage->RLatch();
                //convert page type to B_PLUS_TREE_LEAF_PAGE_TYPE
                leafPage_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(nextPage->GetData());
                //reset index in page is 0
                indexInPage_ = 0;
            }
        }
        return *this;
    }


    INDEX_TEMPLATE_ARGUMENTS
    void INDEXITERATOR_TYPE::UnlockAndUnPin() {
        bufferPoolManager_->FetchPage(leafPage_->GetPageId())->RUnlatch();
        bufferPoolManager_->UnpinPage(leafPage_->GetPageId(), false);
        bufferPoolManager_->UnpinPage(leafPage_->GetPageId(), false);
    }




    template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
    template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
    template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
    template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
    template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
