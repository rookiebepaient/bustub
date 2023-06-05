//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  /* 在缓冲池中创建一个新页面。 将 page_id 设置为新页面的
   id，如果所有帧当前都在使用中且不可清除（换句话说，固定），则设置为 nullptr。
   应该从空闲列表或替换器中选择替换帧（总是首先从空闲列表中找到），然后调用 AllocatePage() 方法来获取新的页面 ID。
   如果替换帧有脏页，应该先把它写回磁盘。 还需要为新页面重置内存和元数据。
   请记住通过调用 replacer.SetEvictable(frame_id, false)
   来“固定”帧，这样替换器就不会在缓冲池管理器“取消固定”它之前驱逐该帧。 另外，请记住在替换器中记录帧的访问历史，以便
   lru-k 算法工作。*/
  // 磁盘上存储的数据被分成大小固定相等的小块叫做帧
  // 系统中的所有内存页面都由 Page 对象表示
  // latch_.lock();      // 加锁
  // 页 与 帧 映射关系，首先初始状态缓存池中。缓存池中有一片连续的内存空间用来存放Page对象
  // free_list_中是 还没有页与其绑定的帧 的集合
  latch_.lock();
  frame_id_t replace_frame_id;
  if (!free_list_.empty()) {
    replace_frame_id = free_list_.front();  // free_list 指示有多少个空闲帧，没有页与之对应的帧称为空闲帧
    free_list_.pop_front();
  } else if (!replacer_->Evict(&replace_frame_id)) {
    latch_.unlock();
    return nullptr;
  }
  page_id_t new_page_id = AllocatePage();
  Page *replace_page = &pages_[replace_frame_id];
  // replace_page->page_id_ = new_page_id;
  if (replace_page->IsDirty()) {
    disk_manager_->WritePage(replace_page->page_id_, replace_page->data_);
  }
  replace_page->page_id_ = new_page_id;
  replace_page->ResetMemory();
  page_table_[new_page_id] = replace_frame_id;
  replace_page->pin_count_++;
  replacer_->SetEvictable(replace_frame_id, false);
  replacer_->RecordAccess(replace_frame_id);
  latch_.unlock();
  return replace_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // 从缓存池中抓取需要的页面
  latch_.lock();  // 加锁
  auto page_iter = page_table_.find(page_id);
  /* 首先在缓冲池中搜索page_id。 如果没有找到，从空闲列表或替换器中选择一个替换帧（总是首先从空闲列表中找到），
    通过调用 disk_manager_->ReadPage() 从磁盘读取页面，并替换帧中的旧页面。
    与 NewPage() 类似，如果旧页面脏了，则需要将其写回磁盘并更新新页面的元数据
    另外，请记住像对 NewPage() 一样禁用驱逐并记录帧的访问历史记录(). */
  // 如果目标页面需要从磁盘抓取但当前所有的帧都在被使用且不可被移除(换句话说就是 固定(pinned)),返回空指针
  if (page_iter != page_table_.end()) {
    // 如果在页表中找到了对应的页
    frame_id_t frame_id = page_iter->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;  // pinned 表示这个frame被某个进程引用了
    replacer_->RecordAccess(frame_id);
    latch_.unlock();  // 解锁
    return page;
  }
  frame_id_t replace_fid;
  // 如果在页表中找不到这个页，那么在free_list中查看有没有空闲帧可以进行绑定
  if (!free_list_.empty()) {
    replace_fid = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&replace_fid)) {
    // 如果在free_list中找不到空闲帧，那么在LRUK替换器中查看是否能找到一个替换帧
    latch_.unlock();  // 解锁
    return nullptr;
  }
  Page *replace_page = &pages_[replace_fid];  // 利用替换帧找到需要被替换的页
  if (replace_page->IsDirty()) {              // 将脏页面写回磁盘
    disk_manager_->WritePage(replace_page->page_id_, replace_page->data_);
  }
  // 将原页表中的数据删除
  page_table_.erase(replace_page->page_id_);
  // 在页表中添加新的映射关系
  page_table_[page_id] = replace_fid;
  // 从磁盘管理器中获取新的页面
  Page *new_page = replace_page;
  disk_manager_->ReadPage(page_id, new_page->data_);
  // 更新新页的信息
  new_page->page_id_ = page_id;
  new_page->pin_count_++;
  new_page->is_dirty_ = false;
  // 将该帧标记为不可移除，并更新访问记录
  replacer_->SetEvictable(replace_fid, false);
  replacer_->RecordAccess(replace_fid);
  latch_.unlock();  // 解锁
  return new_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // 让缓存池中的页取消固定
  // 如果目标页不存在，或者固定数到达1了,直接返回false
  auto page_iter = page_table_.find(page_id);
  if (page_iter == page_table_.end()) {
    return false;
  }
  if (pages_[page_iter->second].GetPinCount() <= 0) {
    return false;
  }
  pages_[page_iter->second].pin_count_--;
  if (pages_[page_iter->second].GetPinCount() == 0) {
    replacer_->SetEvictable(page_iter->second, true);
  }
  if (is_dirty) {
    // 只有当is_dirty为true的时候，才能进行更改is_dirty状态
    pages_[page_iter->second].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // 将指定页面冲刷进磁盘
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t flush_fid = iter->second;
    disk_manager_->WritePage(page_id, pages_[flush_fid].data_);
    // ?? 刷新页面后取消设置脏标识 ??
    if (pages_[flush_fid].IsDirty()) {
      pages_[flush_fid].is_dirty_ = false;
    }
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &page_iter : page_table_) {
    frame_id_t flush_fid = page_iter.second;
    disk_manager_->WritePage(page_iter.first, pages_[flush_fid].data_);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // 如果目标页不在缓冲池中，直接返回true
  if (0U == page_table_.count(page_id)) {
    return true;
  }
  frame_id_t target_fid = page_table_.at(page_id);
  Page *target_page = &pages_[target_fid];
  // 如果目标页在固定状态中，返回false
  if (target_page->GetPinCount() > 0) {
    return false;
  }
  // 从页表中删除目标页
  page_table_.erase(page_id);
  // 停止在替换器中追踪目标页对应帧
  replacer_->Remove(target_fid);
  // 将该帧放回free_list
  free_list_.emplace_back(target_fid);
  // 重置该页的内存和元数据
  target_page->ResetMemory();
  // 将目标页放回数据库
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  ReadPageGuard rpg(this, page);
  return rpg;
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  WritePageGuard wpg(this, page);
  return wpg;
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
