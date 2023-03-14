#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  bpm_->UnpinPage(page_->GetPageId(), page_->IsDirty());
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.Drop();
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (page_ != nullptr) {
    Drop();
  }
}  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  that.Drop();
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.page_->IsDirty());
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.page_ != nullptr) {
    guard_.Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  that.Drop();
  return *this;
}

void WritePageGuard::Drop() {
  guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.page_->IsDirty());
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (guard_.page_ != nullptr) {
    guard_.Drop();
  }
}  // NOLINT

}  // namespace bustub
