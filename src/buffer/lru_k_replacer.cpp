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

    for (auto iter = node_store_.begin(); iter != node_store_.end(); ++iter) {
        if (iter->second->GetK() < k_ && iter->second->GetEvict()) {
            *frame_id = iter->second->GetFrameID();
            break;
        }
    }
    Remove(*frame_id);
    if (!node_store_.count(*frame_id)) {
        return true;
    }
    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    // 如果帧ID超过替换器容量，则说明是无效帧
    if ((size_t)frame_id > replacer_size_) {
        throw ("out of range");
    }
    // 如果帧不存在于访问记录，则在访问记录中记录这次访问
    if (!node_store_.count(frame_id)) {
        // std::shared_ptr<LRUKNode> new_node_sptr = std::make_shared<LRUKNode>();
        // new_node_sptr->GetHistory().emplace_front(current_timestamp_);
        //多模板参数传递的意义就是可以在括号内填入构造函数的参数
        node_store_.emplace(frame_id, std::make_shared<LRUKNode>(frame_id, current_timestamp_));    // 如果使用 = 赋值还有调用LRUNode类的 = 赋值函数
    } else {
        std::shared_ptr<LRUKNode> cur_node = node_store_.at(frame_id);                              // 无序map的 [] 返回的是一个key对应val的引用
        cur_node->SetK(cur_node->GetK() + 1);
        cur_node->GetHistory().emplace_back(current_timestamp_);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if (!node_store_.count(frame_id)) {
        return;
    }
    // 如果帧ID超过替换器容量，则说明是无效帧
    if ((size_t)frame_id > replacer_size_) {
        throw ("out of range");
    }
    std::shared_ptr<LRUKNode> tmp = node_store_.at(frame_id);
    tmp->SetEvict(set_evictable);
    if (tmp->GetEvict()) {
        ++curr_size_;
    } else {
        --curr_size_;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    auto iter = node_store_.find(frame_id);
    if (iter == node_store_.end()) {
        return;
    }
    // 只能移除 evictable 帧
    if (!iter->second->GetEvict()) {
        throw ("can not remove a non-evictable frame");
    }
    // if (node_store_.erase(iter)) {                              //map的移除操作只管移除，不管被移除对象的内存回收等工作
    //     --curr_size_;
    // }
    node_store_.erase(iter);
    --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

}  // namespace bustub
