//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // LRUK替换器的大小代表的是有多少个可丢弃的帧。
  // 初始替换器中没有任何帧，只有当一个帧被标记为可丢弃时，替换器的大小才会增加
  // node_store_的大小跟LRUK替换器的大小无关，因为node_store_存储的是被访问过的所有帧
  // LRUK替换器只存储 需要被丢弃的帧
  if (inf_replacer_.empty() && k_replacer_.empty()) {
    return false;
  }
  if (!inf_replacer_.empty()) {
    *frame_id = inf_replacer_.front();
    Remove(*frame_id);
    inf_replacer_.pop_front();
  } else if (!k_replacer_.empty()) {
    *frame_id = k_replacer_.front();
    Remove(*frame_id);
    k_replacer_.pop_front();
  }
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 如果帧ID超过替换器容量，则说明是无效帧
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw("out of range");
  }
  // 如果帧不存在于访问记录，则在访问记录中记录这次访问
  if (0U == node_store_.count(frame_id)) {
    node_store_.emplace(frame_id,
                        LRUKNode(frame_id, current_timestamp_));  // 如果使用 = 赋值还会调用LRUNode类的 = 赋值函数
  } else {
    // 如果该帧存在，更新访问记录
    node_store_.at(frame_id).SetK(node_store_.at(frame_id).GetK() + 1);
    node_store_.at(frame_id).GetHistory().emplace_back(current_timestamp_);
  }
  if (node_store_.at(frame_id).GetEvict()) {
    if (node_store_.at(frame_id).GetK() < k_) {
      // node_store_.at(frame_id).pos_ = 
      // inf_replacer_.emplace_back(frame_id);
      auto tail = std::prev(inf_replacer_.end(), 1);
      node_store_.at(frame_id).pos_ = inf_replacer_.emplace(tail, frame_id);
    } else {
      // auto tail = std::prev(k_replacer_.end(), 1);
      auto tmp_iter = node_store_.at(frame_id).pos_;
      inf_replacer_.erase(tmp_iter);
      node_store_.at(frame_id).pos_ = k_replacer_.emplace(k_replacer_.end(), frame_id);
    }
  }
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 这个函数控制着替换器的大小
  if (0U == node_store_.count(frame_id)) {
    return;
  }
  // 如果帧ID超过替换器容量，则说明是无效帧
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw("out of range");
  }
  if (node_store_.at(frame_id).GetEvict() && !set_evictable) {
    // 如果这个帧的可移除标记发生了变化，记录下变化，并改变当前LRUK替换器的大小
    node_store_.at(frame_id).SetEvict(set_evictable);
    // 将该页标记为不可移除，在替换器中暂时取消追踪这个页
    if (node_store_.at(frame_id).GetK() < k_) {
      auto tmp_iter = node_store_.at(frame_id).pos_;
      inf_replacer_.erase(tmp_iter);
    } else {
      auto tmp_iter = node_store_.at(frame_id).pos_;
      k_replacer_.erase(tmp_iter);
    }
    --curr_size_;
  } else if (!node_store_.at(frame_id).GetEvict() && set_evictable) {  //
    node_store_.at(frame_id).SetEvict(set_evictable);
    if (node_store_.at(frame_id).GetK() < k_) {
      inf_replacer_.emplace_back(frame_id);
      node_store_.at(frame_id).pos_ = std::prev(inf_replacer_.end(), 1);
    } else {
      k_replacer_.emplace_back(frame_id);
    }
    ++curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  // 只能移除 evictable 帧
  if (!iter->second.GetEvict()) {
    throw("can not remove a non-evictable frame");
  }
  node_store_.erase(iter);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
