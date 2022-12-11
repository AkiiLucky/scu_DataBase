#include "buffer/buffer_pool_manager.h"

namespace scudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}


/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    lock_guard<mutex> lck(latch_);
    //1 search the page from buffer(page table).
    //1.1 if the page exist in buffer(page table), pin the page and return immediately
    Page *target = nullptr;
    if (page_table_->Find(page_id, target))
    {
        target->pin_count_++;
        //erase the page from lru_replacer in order to avoid to the page replaced
        replacer_->Erase(target);
        return target;
    }
    //1.2 if the page not exist in buffer(page table), find a free page or replace an old page
    target = GetVictimPage();
    if (target == nullptr) //not find available page (all page pinned), then return nullptr
    {
        return target;
    }
    //2 if the page for replacement is dirty, write it back to disk.
    if (target->is_dirty_)
    {
        disk_manager_->WritePage(target->GetPageId(), target->data_);
    }
    //3 Delete the old page for replacement from the buffer(page table)
    page_table_->Remove(target->GetPageId());
    //4 Update page metadata, read page content from disk file and insert the new page into buffer(page table)
    disk_manager_->ReadPage(page_id, target->data_);
    target->pin_count_ = 1;
    target->is_dirty_ = false;
    target->page_id_= page_id;
    page_table_->Insert(page_id, target);

    return target; //return page pointer
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    if (!page_table_->Find(page_id, target)) {
        return false;
    }
    //***************************************
    target->is_dirty_ |= is_dirty; //reset is_dirty_
    //***************************************
    assert(target->GetPinCount() > 0);
    target->pin_count_ -= 1;
    if (target->pin_count_ == 0) { //if the page not pinned by any process, insert it into replacer
        replacer_->Insert(target);
    }
    return true;
}


/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    if (!page_table_->Find(page_id, target)) { //if page id is not in page table, return false
        return false;
    }
    assert(target->page_id_ != INVALID_PAGE_ID); //if target is an invalid page then assert

    if (target->is_dirty_) { //if the page is dirty, write back the page to disk
        disk_manager_->WritePage(page_id, target->GetData());
        target->is_dirty_ = false;
    }

    return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    page_table_->Find(page_id, target);
    if (target != nullptr)
    {
        if (target->GetPinCount() > 0) return false;
        replacer_->Erase(target);          //remove it from replacer
        page_table_->Remove(page_id);       //remove it from page_table
        target->is_dirty_= false;
        target->ResetMemory();                  //reset page metadata
        //*****************************************************
        target->page_id_ = INVALID_PAGE_ID;  //reset page id
        //***********************************************************
        free_list_->push_back(target);        //add back to free list
    }
    disk_manager_->DeallocatePage(page_id);     //remove it from disk
    return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    lock_guard<mutex> lck(latch_);

    //1 get a free page from free list or lru_replacer
    Page *target = nullptr;
    target = GetVictimPage();
    if (target == nullptr) return target; //if not find available page (all page pinned), then return nullptr

    //2 if old page is dirty, write back to disk
    if (target->is_dirty_) {
        disk_manager_->WritePage(target->GetPageId(), target->data_);
    }

    //3 update page in page table
    page_table_->Remove(target->GetPageId());
    page_id = disk_manager_->AllocatePage(); //allocate a new page id for new page
    page_table_->Insert(page_id, target);

    //4 reset params and return page pointer
    target->page_id_ = page_id;
    target->ResetMemory(); //reset the metadata of page
    target->is_dirty_ = false;
    target->pin_count_ = 1;

    return target;
}



//get a free page from free list or lru_replacer
Page *BufferPoolManager::GetVictimPage() {
    Page *page = nullptr;
    if (!free_list_->empty()) //free_list have free page
    {
        page = free_list_->front();
        free_list_->pop_front();
        assert(page->GetPageId() == INVALID_PAGE_ID);
    }
    else  //no free page in free_list
    {
        if (replacer_->Size() == 0) //can not replace any page, because all page pinned by process
        {
            return nullptr;
        }
        replacer_->Victim(page); //replace a page from replacer
    }
    assert(page->GetPinCount() == 0);
    return page;
}



//************************************************

//DEBUG

bool BufferPoolManager::CheckAllUnpined() {
    bool res = true;
    for (size_t i = 1; i < pool_size_; i++)
    {
        if (pages_[i].pin_count_ != 0)
        {
            res = false;
            std::cout<<"page "<<pages_[i].page_id_<<" pin count:"<<pages_[i].pin_count_<<endl;
        }

    }
    return res;
}

//************************************************



/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
//    bool BufferPoolManager::DeletePage(page_id_t page_id) {
//        lock_guard<mutex> lck(latch_);
//        Page *tar = nullptr;
//        page_table_->Find(page_id,tar);
//        if (tar != nullptr) {
//            if (tar->GetPinCount() > 0) {
//                //     cout<<"DeletePage error"<<tar->page_id_<<endl;
////      assert(false);
//                return false;
//            }
//            replacer_->Erase(tar);
//            page_table_->Remove(page_id);
//            tar->is_dirty_= false;
//            tar->ResetMemory();
//            tar->page_id_ = INVALID_PAGE_ID;
//            free_list_->push_back(tar);
//        }
//        disk_manager_->DeallocatePage(page_id);
//        return true;
//    }


} // namespace scudb
