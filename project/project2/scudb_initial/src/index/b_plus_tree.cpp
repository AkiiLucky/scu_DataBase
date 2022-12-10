/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"


namespace scudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockedCnt = 0;

/*****************************************************************************
 * SEARCH todo
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    //1.find leaf page from b+ tree
    B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage = FindLeafPage(key,false,OperationType::READ,transaction);
    if (leafPage == nullptr) {
        return false;
    }
    //2.find value from leaf page
    result.resize(1);
    auto res = leafPage->Lookup(key,result[0],comparator_);
    //3.unPin the leaf page from buffer pool
    page_id_t pageId = leafPage->GetPageId();
    this->FreePagesInTransaction(false,transaction,pageId);
    return res;
}

/*****************************************************************************
 * INSERTION todo
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    this->LockRootPageId(true); //avoid root page id changed by other transaction
    if (this->IsEmpty()) {
        this->StartNewTree(key,value); //create a new tree
        this->TryUnlockRootPageId(true); //unlock the exclusive lock of root page id
        return true;
    }
    this->TryUnlockRootPageId(true); //unlock the exclusive lock of root page id
    bool res = this->InsertIntoLeaf(key,value,transaction); //insert key & value pair into leaf page
    return res;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {

    //1.ask for a new page from buffer pool manager
    page_id_t rootPageId;
    Page *rootPage = buffer_pool_manager_->NewPage(rootPageId);
    if(rootPage == nullptr) { //memory is not enough, return false
        return false;
    };

    //2.get root page pointer
    B_PLUS_TREE_LEAF_PAGE_TYPE *rootPagePointer = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootPage->GetData());

    //3.update root page id
    rootPagePointer->Init(rootPageId,INVALID_PAGE_ID);
    root_page_id_ = rootPageId;
    UpdateRootPageId(true);

    //4.insert key & value pair into leaf page
    rootPagePointer->Insert(key,value,comparator_);

    //5.unpin root page and set it dirty
    buffer_pool_manager_->UnpinPage(rootPageId,true);

    //6.start new tree success, return true
    return true;
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    //1.get leaf page to insert
    B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage = FindLeafPage(key,false,OperationType::INSERT,transaction);

    //2.if the key exists in leaf page, return false
    ValueType v;
    if (leafPage->Lookup(key,v,comparator_)) {
        FreePagesInTransaction(true,transaction);
        return false;
    }

    //3.if the key not exists in leaf page, insert it into leaf page
    leafPage->Insert(key,value,comparator_);

    //4.if size > max size, split the page
    int pageSize = leafPage->GetSize();
    int pageMaxSize = leafPage->GetMaxSize();
    if (pageSize > pageMaxSize) {
        B_PLUS_TREE_LEAF_PAGE_TYPE *newLeafPage = Split(leafPage,transaction);
        KeyType firstKey = newLeafPage->KeyAt(0);
        InsertIntoParent(leafPage,firstKey,newLeafPage,transaction);
    }

    //5.unpin relevant pages and return true
    FreePagesInTransaction(true,transaction);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
//INDEX_TEMPLATE_ARGUMENTS
//template <typename N> N *BPLUSTREE_TYPE::Split(N *node) { return nullptr; }

INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *pagePointer, Transaction *transaction) {

    assert(pagePointer != nullptr && transaction != nullptr);

    //1.get a new page from buffer pool manager
    page_id_t newPageId;
    Page* const newPage = buffer_pool_manager_->NewPage(newPageId);
    assert(newPage != nullptr);

    //2.get writing lock of the page
    newPage->WLatch();
    transaction->AddIntoPageSet(newPage);

    //3.move half pairs from old page to new page
    N *newPagePointer = reinterpret_cast<N *>(newPage->GetData());
    newPagePointer->Init(newPageId, pagePointer->GetParentPageId());
    pagePointer->MoveHalfTo(newPagePointer, buffer_pool_manager_);

    //4.return new page pointer
    return newPagePointer;
}


/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {

    assert(old_node != nullptr && new_node != nullptr && transaction != nullptr);
    if (old_node->IsRootPage())
    {
        //old node is root node
        //1.get a new page from buffer pool
        Page* const newPage = buffer_pool_manager_->NewPage(root_page_id_);
        assert(newPage != nullptr);
        assert(newPage->GetPinCount() == 1);
        //2.create a new root page
        B_PLUS_TREE_INTERNAL_PAGE *newRoot = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(newPage->GetData());
        newRoot->Init(root_page_id_);
        newRoot->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
        //3.reset parent page id
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        UpdateRootPageId();
        //4.unpin the new page and set it dirty
        buffer_pool_manager_->UnpinPage(newRoot->GetPageId(),true);
    }
    else
    {
        //old node is not root node
        //1.pin parent page
        page_id_t parentId = old_node->GetParentPageId();
        auto *parentPage = FetchPage(parentId);
        assert(parentPage != nullptr);
        //2.get parent page pointer
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parentPage);
        //3.reset new node parent id
        new_node->SetParentPageId(parentId);
        //4.insert new node
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        //5.if parent page size > max size, split parent page
        if (parent->GetSize() > parent->GetMaxSize())
        {
            B_PLUS_TREE_INTERNAL_PAGE *newLeafPage = Split(parent,transaction);
            KeyType firstKey = newLeafPage->KeyAt(0); //get first key from newLeafPage
            InsertIntoParent(parent,firstKey,newLeafPage,transaction); //insert first key
        }
        //6.unpin parent page and set it dirty
        buffer_pool_manager_->UnpinPage(parentId,true);
    }

}

/*****************************************************************************
 * REMOVE todo
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    if (this->IsEmpty()) {
        return ; //empty tree, return
    } else {
        //get page to delete pairs
        B_PLUS_TREE_LEAF_PAGE_TYPE *pageToDeleteItem = this->FindLeafPage(key,false,OperationType::DELETE,transaction);
        //if page size < min size, coalesce or redistribute
        int pageSize = pageToDeleteItem->RemoveAndDeleteRecord(key,comparator_);
        int minPageSize = pageToDeleteItem->GetMinSize();
        if (pageSize < minPageSize) {
            this->CoalesceOrRedistribute(pageToDeleteItem,transaction);
        }
        //unpin all pages from current transaction
        this->FreePagesInTransaction(true,transaction);
    }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    //1.the node is root
    if (node->IsRootPage()) {
        //if the node is root, make the child become new root and delete the node
        bool OldRootNeedDelete = AdjustRoot(node);
        if (OldRootNeedDelete) {
            transaction->AddIntoDeletedPageSet(node->GetPageId()); //delete node
        }
        return OldRootNeedDelete;
    }

    bool ret;
    //2.the node is not root node, let nearNode become the previous or next child
    N *nearNode;
    bool isRightNode = FindLeftSibling(node,nearNode,transaction);
    //pin parent page
    BPlusTreePage *parent = FetchPage(node->GetParentPageId());
    //get parent page
    B_PLUS_TREE_INTERNAL_PAGE *parentPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent);
    //get page size
    int nodeSize = node->GetSize();
    int nearNodeSize = nearNode->GetSize();
    int nodeMaxSize = node->GetMaxSize();
    if (nodeSize + nearNodeSize <= nodeMaxSize)  //if a single node can accommodate node and nearNode
    {
        //make node right
        if (isRightNode) swap(node,nearNode);
        //get index to remove
        int removeIndex = parentPage->ValueIndex(node->GetPageId());
        //merge node and nearNode
        Coalesce(nearNode,node,parentPage,removeIndex,transaction);
        ret =  true;
    }
    else
    {
        //if a single node can not accommodate node and nearNode, redistribute the node and nearNode
        int nodeInParentIndex = parentPage->ValueIndex(node->GetPageId());
        Redistribute(nearNode,node,nodeInParentIndex);
        ret = false;
    }
    //unpin parent page
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), ret);
    return ret;

}


INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindLeftSibling(N *node, N * &sibling, Transaction *transaction) {
    //pin parent page
    auto parentPage = FetchPage(node->GetParentPageId());
    B_PLUS_TREE_INTERNAL_PAGE *parentPagePointer = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parentPage);
    //get index from parent page
    int idx = parentPagePointer->ValueIndex(node->GetPageId());
    //get sibling node index
    int siblingIndex;
    if (idx == 0) siblingIndex = idx + 1; //no left sibling
    else siblingIndex = idx - 1; //have left sibling
    //update sibling node from sibling node index
    sibling = reinterpret_cast<N *>(CrabingProtocalFetchPage(parentPagePointer->ValueAt(siblingIndex),OperationType::DELETE,-1,transaction));
    //unpin parent page
    buffer_pool_manager_->UnpinPage(parentPagePointer->GetPageId(), false);
    //if sibling is right return true, else return false
    return idx == 0;
}


/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *&neighbor_node,
                              N *&node,
                              BPlusTreeInternalPage<KeyType,
                              page_id_t,
                              KeyComparator> *&parent,
                              int index,
                              Transaction *transaction) {

    //assumption neighbor_node is before node
    assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
    //move later one to previous one
    node->MoveAllTo(neighbor_node,index,buffer_pool_manager_);
    transaction->AddIntoDeletedPageSet(node->GetPageId());
    parent->Remove(index);
    if (parent->GetSize() <= parent->GetMinSize()) {
        return CoalesceOrRedistribute(parent,transaction);
    }
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    if (index == 0) {
        neighbor_node->MoveFirstToEndOf(node,buffer_pool_manager_);
    } else {
        neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->IsLeafPage()) {// case 2
        assert(old_root_node->GetSize() == 0);
        assert (old_root_node->GetParentPageId() == INVALID_PAGE_ID);
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();
        return true;
    }
    if (old_root_node->GetSize() == 1) {// case 1
        B_PLUS_TREE_INTERNAL_PAGE *root = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
        const page_id_t newRootId = root->RemoveAndReturnOnlyChild();
        root_page_id_ = newRootId;
        UpdateRootPageId();
        // set the new root's parent id "INVALID_PAGE_ID"
        Page *page = buffer_pool_manager_->FetchPage(newRootId);
        assert(page != nullptr);
        B_PLUS_TREE_INTERNAL_PAGE *newRoot =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        newRoot->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(newRootId, true);
        return true;
    }
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR todo
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
    KeyType useless;
    auto start_leaf = FindLeafPage(useless, true);
    TryUnlockRootPageId(false);
    return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
    auto start_leaf = FindLeafPage(key);
    TryUnlockRootPageId(false);
    if (start_leaf == nullptr) {
        return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
    }
    int idx = start_leaf->KeyIndex(key,comparator_);
    return INDEXITERATOR_TYPE(start_leaf, idx, buffer_pool_manager_);
}




/*****************************************************************************
 * UTILITIES AND DEBUG todo
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
//INDEX_TEMPLATE_ARGUMENTS
//B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
//                                                         bool leftMost) {
//  return nullptr;
//}
//********************************************************************************

    INDEX_TEMPLATE_ARGUMENTS
    B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                             bool leftMost,OperationType op,
                                                             Transaction *transaction) {
        bool exclusive = (op != OperationType::READ);
        LockRootPageId(exclusive);
        if (IsEmpty()) {
            TryUnlockRootPageId(exclusive);
            return nullptr;
        }
        //, you need to first fetch the page from buffer pool using its unique page_id, then reinterpret cast to either
        // a leaf or an internal page, and unpin the page after any writing or reading operations.
        auto pointer = CrabingProtocalFetchPage(root_page_id_,op,-1,transaction);
        page_id_t next;
        for (page_id_t cur = root_page_id_; !pointer->IsLeafPage(); pointer =
                                                                            CrabingProtocalFetchPage(next,op,cur,transaction),cur = next) {
            B_PLUS_TREE_INTERNAL_PAGE *internalPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(pointer);
            if (leftMost) {
                next = internalPage->ValueAt(0);
            }else {
                next = internalPage->Lookup(key,comparator_);
            }
        }
        return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(pointer);
    }



    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
        auto page = buffer_pool_manager_->FetchPage(page_id);
        return reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *BPLUSTREE_TYPE::CrabingProtocalFetchPage(page_id_t page_id,OperationType op,page_id_t previous, Transaction *transaction) {
        bool exclusive = op != OperationType::READ;
        auto page = buffer_pool_manager_->FetchPage(page_id);
        Lock(exclusive,page);
        auto treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());
        if (previous > 0 && (!exclusive || treePage->IsSafeOperation(op))) {
            FreePagesInTransaction(exclusive,transaction,previous);
        }
        if (transaction != nullptr)
            transaction->AddIntoPageSet(page);
        return treePage;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t cur) {
        TryUnlockRootPageId(exclusive);
        if (transaction == nullptr) {
            assert(!exclusive && cur >= 0);
            Unlock(false,cur);
            buffer_pool_manager_->UnpinPage(cur,false);
            return;
        }
        for (Page *page : *transaction->GetPageSet()) {
            int curPid = page->GetPageId();
            Unlock(exclusive,page);
            buffer_pool_manager_->UnpinPage(curPid,exclusive);
            if (transaction->GetDeletedPageSet()->find(curPid) != transaction->GetDeletedPageSet()->end()) {
                buffer_pool_manager_->DeletePage(curPid);
                transaction->GetDeletedPageSet()->erase(curPid);
            }
        }
        assert(transaction->GetDeletedPageSet()->empty());
        transaction->GetPageSet()->clear();
    }
//************************************************************************************


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
    INDEX_TEMPLATE_ARGUMENTS
    std::string BPLUSTREE_TYPE::ToString(bool verbose) {
        if (IsEmpty()) {
            return "Empty tree";
        }
        std::queue<BPlusTreePage *> todo, tmp;
        std::stringstream tree;
        auto node = reinterpret_cast<BPlusTreePage *>(
                buffer_pool_manager_->FetchPage(root_page_id_));
        if (node == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        }
        todo.push(node);
        bool first = true;
        while (!todo.empty()) {
            node = todo.front();
            if (first) {
                first = false;
                tree << "| ";
            }
            // leaf page, print all key-value pairs
            if (node->IsLeafPage()) {
                auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
                tree << page->ToString(verbose) <<"("<<node->GetPageId()<< ")| ";
            } else {
                auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
                tree << page->ToString(verbose) <<"("<<node->GetPageId()<< ")| ";
                page->QueueUpChildren(&tmp, buffer_pool_manager_);
            }
            todo.pop();
            if (todo.empty() && !tmp.empty()) {
                todo.swap(tmp);
                tree << '\n';
                first = true;
            }
            // unpin node when we are done
            buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
        }
        return tree.str();
    }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}







//*******************************************************************************************
/***************************************************************************
 *  Check integrity of B+ tree data structure.
 ***************************************************************************/

    INDEX_TEMPLATE_ARGUMENTS
    int BPLUSTREE_TYPE::isBalanced(page_id_t pid) {
        if (IsEmpty()) return true;
        auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
        if (node == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,"all page are pinned while isBalanced");
        }
        int ret = 0;
        if (!node->IsLeafPage())  {
            auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(node);
            int last = -2;
            for (int i = 0; i < page->GetSize(); i++) {
                int cur = isBalanced(page->ValueAt(i));
                if (cur >= 0 && last == -2) {
                    last = cur;
                    ret = last + 1;
                }else if (last != cur) {
                    ret = -1;
                    break;
                }
            }
        }
        buffer_pool_manager_->UnpinPage(pid,false);
        return ret;
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::isPageCorr(page_id_t pid,pair<KeyType,KeyType> &out) {
        if (IsEmpty()) return true;
        auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
        if (node == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,"all page are pinned while isPageCorr");
        }
        bool ret = true;
        if (node->IsLeafPage())  {
            auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
            int size = page->GetSize();
            ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
            for (int i = 1; i < size; i++) {
                if (comparator_(page->KeyAt(i-1), page->KeyAt(i)) > 0) {
                    ret = false;
                    break;
                }
            }
            out = pair<KeyType,KeyType>{page->KeyAt(0),page->KeyAt(size-1)};
        } else {
            auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
            int size = page->GetSize();
            ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
            pair<KeyType,KeyType> left,right;
            for (int i = 1; i < size; i++) {
                if (i == 1) {
                    ret = ret && isPageCorr(page->ValueAt(0),left);
                }
                ret = ret && isPageCorr(page->ValueAt(i),right);
                ret = ret && (comparator_(page->KeyAt(i) ,left.second)>0 && comparator_(page->KeyAt(i), right.first)<=0);
                ret = ret && (i == 1 || comparator_(page->KeyAt(i-1) , page->KeyAt(i)) < 0);
                if (!ret) break;
                left = right;
            }
            out = pair<KeyType,KeyType>{page->KeyAt(0),page->KeyAt(size-1)};
        }
        buffer_pool_manager_->UnpinPage(pid,false);
        return ret;
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::Check(bool forceCheck) {
        if (!forceCheck && !openCheck) {
            return true;
        }
        pair<KeyType,KeyType> in;
        bool isPageInOrderAndSizeCorr = isPageCorr(root_page_id_, in);
        bool isBal = (isBalanced(root_page_id_) >= 0);
        bool isAllUnpin = buffer_pool_manager_->CheckAllUnpined();
        if (!isPageInOrderAndSizeCorr) cout<<"problem in page order or page size"<<endl;
        if (!isBal) cout<<"problem in balance"<<endl;
        if (!isAllUnpin) cout<<"problem in page unpin"<<endl;
        return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
    }

    //********************************************************************************************************************


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb



