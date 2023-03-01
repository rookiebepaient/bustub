#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code: 伪码
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  // throw NotImplementedException("TrieStore::Get is not implemented.");

  // 先对根节点上锁,得到根节点,释放根节点,在持有根节点锁时不要在字典树中查找元素
  // 先对根节点上锁再访问根节点，然后释放根节点
  root_lock_.try_lock();
  Trie new_root = Trie(root_);
  // TrieNode *new_root = temp_root;
  root_lock_.unlock();

  const auto *result = new_root.Get<T>(key);
  if (result == nullptr) {
    return std::nullopt;
  }
  ValueGuard<T> ret = ValueGuard<T>(new_root, *result);
  return ret;
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Put is not implemented.");

  // 注意T可能是一个不可复制的类型，为value创建一个shared_ptr时使用 std::move
  // 只需要加写锁就可以了， 为什么?
  write_lock_.lock();
  Trie new_root = Trie(root_);
  root_ = new_root.Put<T>(key, std::move(value));
  write_lock_.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Remove is not implemented.");

  write_lock_.lock();
  Trie new_root = Trie(root_);
  root_ = new_root.Remove(key);
  write_lock_.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
