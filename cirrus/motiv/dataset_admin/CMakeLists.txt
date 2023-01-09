# Used to generate datasets for experiments
add_executable(dataset_admin
  dataset_admin.cc
  dataset_admin.h
  dataset_admin_main.cc
)
target_link_libraries(dataset_admin PRIVATE cirrus gflags)
