#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  // 所有的操作都不应该在原树上进行,尽可能的复用现有存在的节点,并且创建新的节点来代表新树
  // 智能指针shared_ptr的使用 创建一个新的节点,使用Clone()函数 在新树上复用一个现有的节点,可以拷贝

  std::shared_ptr<const bustub::TrieNode> temp_root = root_;
  for (const char &c : key) {
    if (temp_root->children_.count(c)) {
      temp_root = temp_root->children_.at(c);
    } else {
      return nullptr;
    }
  }
  // 如果字典树中没有key,则返回nullptr
  if (!temp_root->is_value_node_) {  // 格式检查提示使用 ! 来代替 not
    return nullptr;                  // c++11 新特性 多使用nullptr来代替NULL或者0
  }
  // 当初始化一个转换值时，使用auto来避免复制类型名
  const auto *target_node = dynamic_cast<const TrieNodeWithValue<T> *>(temp_root.get());  // 动态转换
  // 如果字典树中有key,但是类型不匹配,返回nullptr
  if (target_node == nullptr) {
    return nullptr;
  }
  // 其他情况代表正常获得了目标值,返回该指针
  T *res = target_node->value_.get();
  return res;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  /* 任何时候都需要将当前节点克隆一份出来进行修改，然后再将节点接回去
    遍历keyword中的字符Char
      if 当前节点的children没有字符Char
        生成一个新节点
        在当前节点的children中加入一个字符Char的新节点
      节点移动至字符Char的节点

    需要记录根节点的位置和需要插入节点的祖先节点位置
  */
  // ！！！移动一个临时对象避免复制消除
  // 不要使用 std::move
  std::shared_ptr<bustub::TrieNode> new_root = std::shared_ptr<TrieNode>(root_->Clone());
  if (key.empty()) {
    std::shared_ptr<T> val_ptr = std::make_shared<T>(std::move(value));
    TrieNodeWithValue<T> new_nwv(new_root->children_, val_ptr);
    new_root = std::make_shared<TrieNodeWithValue<T>>(std::move(new_nwv));
    return Trie(new_root);
  }
  // children_ 字段map中shared_ptr指向的是const TrieNode, 不能复制给非const TrieNode
  // 所有的操作都不应该在原树上进行,尽可能的复用现有存在的节点,并且创建新的节点来代表新树
  // 字典树的初始状态是一个空指针, map的作用是key 指示当前节点的下一个字符有哪些, value 指示下一个字符对应的节点

  std::shared_ptr<TrieNode> cur_node = new_root;
  auto ch_iter = key.begin();
  auto last_char = key.end() - 1;
  // std::vector<std::shared_ptr<TrieNode>> char_trace{};         //如果存储的是指针, 那么清空数组的时候会重复删除指针
  while (ch_iter != last_char) {  // 这里不能使用constexpr
    auto iter = cur_node->children_.find(*ch_iter);
    // 如果该节点不存在，创建一个新的节点，并插入
    if (iter == cur_node->children_.end()) {
      TrieNode empty_node;
      std::shared_ptr<TrieNode> empty_node_ptr = std::make_shared<TrieNode>(std::move(empty_node));
      cur_node->children_.emplace(*ch_iter, empty_node_ptr);
      cur_node = empty_node_ptr;
    } else {
      // ！！！移动一个临时对象避免复制消除
      // 不要使用 std::move
      std::shared_ptr<TrieNode> new_node = std::shared_ptr<TrieNode>((*iter).second->Clone());
      cur_node->children_[*ch_iter] = new_node;
      cur_node = new_node;
    }
    ch_iter++;
  }

  // 这里的分情况讨论是课程的提示，自己想的时候逻辑很混乱
  // 如果相应节点不存在，创建一个终结节点 TrieNodeWithValue，插入成功；
  // 如果相应节点存在但不是终结节点（通过 is_end_ 判断），将其转化为 TrieNodeWithValue
  // 并把值赋给该节点，该操作不破坏以该键为前缀的后续其它节点（children_ 不变），插入成功；
  // 如果相应节点存在且是终结节点，说明该键在 Trie 树存在，规定不能覆盖已存在的值，返回插入失败。

  // 此时的ch_iter代表要有节点的值, cur_node 是待插入值节点的父节点
  // 为value创建一个智能指针
  std::shared_ptr<T> val_ptr = std::make_shared<T>(std::move(value));
  auto cur_node_iter = cur_node->children_.find(*last_char);
  auto iter_end = cur_node->children_.end();
  std::shared_ptr<TrieNodeWithValue<T>> val_node;
  if (cur_node_iter != iter_end) {
    val_node = std::make_shared<TrieNodeWithValue<T>>(
        std::move(TrieNodeWithValue<T>(cur_node->children_.at(*last_char)->children_, val_ptr)));
  } else {
    val_node = std::make_shared<TrieNodeWithValue<T>>(std::move(TrieNodeWithValue<T>(val_ptr)));
  }
  cur_node->children_[*last_char] = val_node;

  /* std::shared_ptr<const TrieNode> tavsal = new_root;
  auto iter = key.begin();
  while (iter != key.end()) {
    const TrieNodeWithValue<T> *target_node =
          dynamic_cast<const TrieNodeWithValue<T> *>(tavsal.get());
    if (target_node != nullptr) {
      std::cout << target_node->value_ << std::endl;
    }
    tavsal = tavsal->children_.at(*iter);
    iter++;
  } */

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  // 逻辑很重要
  // remove中有以下几个情形
  // 1. 如果 key 为空, 则直接对根节点进行操作
  // 2. 如果 key 在树中不存在，直接返回根节点
  // 3. 如果 key 在树中存在，
  //   1）将该节点的值删除, 将该值变为TrieNode 类型
  //   2）如果该节点没有孩子, 移除该节点
  // 非const变量可以赋值给const变量，反之不行

  // ！！！移动一个临时对象避免复制消除
  // 不要使用 std::move
  std::shared_ptr<bustub::TrieNode> new_root = std::shared_ptr<TrieNode>(root_->Clone());
  std::vector<std::shared_ptr<TrieNode>> node_tarce;
  std::shared_ptr<TrieNode> cur_node = new_root;
  bool is_del_node = true;
  // 1. 如果 key 为空, 则直接对根节点进行操作
  if (key.empty()) {
    std::shared_ptr<bustub::TrieNode> new_root = std::shared_ptr<TrieNode>(root_->Clone());
    TrieNode new_node(new_root->children_);
    new_root = std::make_shared<TrieNode>(std::move(new_node));
    return Trie(new_root);
  }

  auto char_iter = key.begin();
  while (char_iter != key.end()) {
    auto node_iter = cur_node->children_.find(*char_iter);
    if (node_iter != cur_node->children_.end()) {
      node_tarce.emplace_back(cur_node->Clone());
      cur_node = std::const_pointer_cast<TrieNode>(node_iter->second);
    } else {
      // 2. 如果 key 在树中不存在，直接返回根节点
      return Trie(new_root);
    }
    char_iter++;
  }
  // 2. 如果 key 在树中不存在，直接返回根节点
  if (!cur_node->is_value_node_) {
    return Trie(new_root);
  }
  for (auto path_it = node_tarce.rbegin(); path_it != node_tarce.rend(); path_it++) {
    std::shared_ptr<TrieNode> node;
    if (is_del_node) {
      node = std::make_shared<TrieNode>(TrieNode(cur_node->children_));
    } else {
      node = cur_node;
    }
    (*path_it)->children_[*(--char_iter)] = node;
    if (node->children_.empty() && is_del_node) {
      (*path_it)->children_.erase(*(char_iter));
    }
    cur_node = *path_it;
    if (cur_node->is_value_node_) {
      is_del_node = false;
    }
  }
  std::shared_ptr<TrieNode> ret =
      node_tarce.empty() ? nullptr : std::shared_ptr<TrieNode>(std::move(node_tarce.front()));
  return Trie(ret);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
