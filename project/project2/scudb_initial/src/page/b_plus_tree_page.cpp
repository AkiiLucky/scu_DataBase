/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace scudb {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const {
    return this->page_type_ == IndexPageType::LEAF_PAGE;
}
bool BPlusTreePage::IsRootPage() const {
    return this->parent_page_id_ == INVALID_PAGE_ID;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) {
    this->page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const {
    return this->size_;
}
void BPlusTreePage::SetSize(int size) {
    this->size_ = size;
}
void BPlusTreePage::IncreaseSize(int amount) {
    this->size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const {
    return this->max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
    this->max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const {
    if (IsRootPage()) {
        if (IsLeafPage()) {
            //if the tree only has a page and a pointer, it is empty tree, then return 1;
            return 1;
        } else {
            //there are at least 1 node in root, return 2
            return 2;
        }
    }
    return (this->max_size_ ) / 2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const {
    return this->parent_page_id_;
}
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
    this->parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const {
    return this->page_id_;
}
void BPlusTreePage::SetPageId(page_id_t page_id) {
    this->page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

/*
 * for concurrent index, define the function to ensure the safe Operation
 */
bool BPlusTreePage::IsSafeOperation(scudb::OperationType operation) {
    if (operation == OperationType::READ) {
        return true;
    }
    int size = this->GetSize();
    int maxSize = this->GetMaxSize();
    if (operation == OperationType::INSERT) {
        return size < maxSize;
    }
    int minSize = GetMinSize() + 1;
    if (operation == OperationType::DELETE) {
        if (IsLeafPage()) {
            return size >= minSize;
        } else {
            return size > minSize;
        }
    }
    assert(false);//invalid operation
}

} // namespace scudb


