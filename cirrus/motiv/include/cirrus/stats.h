#pragma once

#include <cstdint>
#include <mutex>

namespace cirrus {

// This class stores counters used by Cirrus.
class Stats {
 public:
  static Stats& Local() {
    static thread_local Stats local;
    return local;
  }

  template <typename Callable>
  static void RunOnGlobal(const Callable& c) {
    std::unique_lock<std::mutex> lock(class_mutex_);
    c(global_);
  }

  Stats(const Stats&) = delete;
  Stats& operator=(const Stats&) = delete;

  uint64_t GetInventoryNotifications() const { return inventory_notifications_; }
  uint64_t GetSalesNotifications() const { return sales_notifications_; }
  uint64_t GetHotInventoryDrops() const { return hot_inventory_drops_; }
  uint64_t GetHotSalesDrops() const { return hot_sales_drops_; }
  uint64_t GetViewMaintInits() const { return view_maint_inits_; }
  uint64_t GetReadWithPause() const { return read_with_pause_; }
  uint64_t GetReadWithoutPause() const { return read_without_pause_; }
  uint64_t GetManualViewMaints() const { return manual_view_maints_; }

  void BumpInventoryNotifications() { ++inventory_notifications_; }
  void BumpSalesNotifications() { ++sales_notifications_; }
  void BumpHotInventoryDrops() { ++hot_inventory_drops_; }
  void BumpHotSalesDrops() { ++hot_sales_drops_; }
  void BumpViewMaintInits() { ++view_maint_inits_; }
  void BumpReadWithPause() { ++read_with_pause_; }
  void BumpReadWithoutPause() { ++read_without_pause_; }
  void BumpManualViewMaints() { ++manual_view_maints_; }

  // Threads must call this method to post their counter values to the global
  // `Stats` instance.
  void PostToGlobal() const;
  void Reset();

 private:
  Stats();

  static std::mutex class_mutex_;
  static Stats global_;

  uint64_t inventory_notifications_;
  uint64_t sales_notifications_;

  uint64_t hot_inventory_drops_;
  uint64_t hot_sales_drops_;

  uint64_t view_maint_inits_;
  uint64_t manual_view_maints_;

  uint64_t read_with_pause_;
  uint64_t read_without_pause_;
};

}  // namespace cirrus
