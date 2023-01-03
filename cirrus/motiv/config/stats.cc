#include "cirrus/stats.h"

namespace cirrus {

std::mutex Stats::class_mutex_;
Stats Stats::global_;

Stats::Stats() { Reset(); }

void Stats::PostToGlobal() const {
  std::unique_lock<std::mutex> lock(class_mutex_);
  global_.inventory_notifications_ += inventory_notifications_;
  global_.sales_notifications_ += sales_notifications_;
  global_.hot_inventory_drops_ += hot_inventory_drops_;
  global_.hot_sales_drops_ += hot_sales_drops_;
  global_.view_maint_inits_ += view_maint_inits_;
  global_.read_with_pause_ += read_with_pause_;
  global_.read_without_pause_ += read_without_pause_;
  global_.manual_view_maints_ += manual_view_maints_;
}

void Stats::Reset() {
  inventory_notifications_ = 0;
  sales_notifications_ = 0;
  hot_inventory_drops_ = 0;
  hot_sales_drops_ = 0;
  view_maint_inits_ = 0;
  read_with_pause_ = 0;
  read_without_pause_ = 0;
  manual_view_maints_ = 0;
}

}  // namespace cirrus
