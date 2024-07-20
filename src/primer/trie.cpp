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
  // std::cout << "[" <<  __func__ << "]KEY: " << key << std::endl;
  if (key.empty()) {
    if (!this->GetRoot() || !this->GetRoot()->is_value_node_) {
      return nullptr;
    }
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(root_.get());
    return value_node ? value_node->value_.get() : nullptr;
  }
  std::shared_ptr<const TrieNode> node = this->GetRoot();
  if (!node) {
    return nullptr;
  }
  for (char c : key) {
    auto it = node->children_.find(c);
    if (it == node->children_.end()) {
      return nullptr;
    }
    node = it->second;
  }
  std::shared_ptr<const TrieNodeWithValue<T>> last = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(node);
  return last ? last->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  if (key.empty()) {
    auto new_root = std::make_shared<TrieNodeWithValue<T>>(
        this->GetRoot() ? this->GetRoot()->children_ : std::map<char, std::shared_ptr<const TrieNode>>(),
        std::make_shared<T>(std::move(value)));
    return Trie(new_root);
  }
  std::unique_ptr<TrieNode> new_root = this->GetRoot() ? this->GetRoot()->Clone() : std::make_unique<TrieNode>();
  TrieNode *node = new_root.get();
  TrieNode *pre;

  for (char c : key) {
    pre = node;
    if (node->children_.find(c) == node->children_.end()) {
      // didn't find the key in children, create a new children
      auto new_node = std::make_shared<TrieNode>();
      node->children_[c] = new_node;
      node = new_node.get();
    } else {
      // found a matched children for the key, copy itself
      auto new_node = node->children_[c].get()->Clone();
      auto new_node_ptr = new_node.get();
      node->children_[c] = std::shared_ptr<const TrieNode>(std::move(new_node));
      node = new_node_ptr;
    }
  }

  // change the last node to type TrieNodeWithValue
  auto last_node = std::make_shared<TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)));
  pre->children_[key.back()] = last_node;

  return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (key.empty()) {
    if (!this->GetRoot() || !this->GetRoot()->is_value_node_) {
      return *this;
    }
    auto new_root = std::make_shared<TrieNode>(this->GetRoot()->children_);
    new_root->is_value_node_ = false;
    return Trie(new_root);
  }

  std::unique_ptr<TrieNode> new_root = this->GetRoot()->Clone();
  TrieNode *node = new_root.get();
  std::vector<std::pair<TrieNode *, char>> path;

  for (char c : key) {
    if (node->children_.find(c) == node->children_.end()) {
      return *this;
    }
    path.emplace_back(node, c);
    // std::cout << "c value " << c << std::endl;
    // std::cout << "Before clone: " << node->children_[c]->is_value_node_ << std::endl;
    // if is the node to be removed, construct a TrieNode type
    // if not, do not change its type, call Clone()
    std::unique_ptr<TrieNode> new_node =
        (c == key.back()) ? std::make_unique<TrieNode>(node->children_[c]->children_) : node->children_[c]->Clone();
    new_node->is_value_node_ = node->children_[c]->is_value_node_;
    // std::cout << "After clone: " << new_node->is_value_node_ << std::endl;
    auto new_node_ptr = new_node.get();
    node->children_[c] = std::shared_ptr<const TrieNode>(std::move(new_node));
    node = new_node_ptr;
  }

  if (node->is_value_node_) {
    node->is_value_node_ = false;
    // std::cout << "path size " << path.size() << std::endl;
    for (auto it = path.rbegin(); it != path.rend(); ++it) {
      auto [parent, ch] = *it;
      if (node->children_.empty() && !node->is_value_node_) {
        // std::cout << "erase ch " << ch << "\n";
        parent->children_.erase(ch);
      } else {
        break;
      }
      node = parent;
    }
    if (new_root->children_.empty() && !new_root->is_value_node_) {
      // std::cout << "This is a empty trie now" << std::endl;
      return {};
    }
    return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
  }

  return *this;
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
