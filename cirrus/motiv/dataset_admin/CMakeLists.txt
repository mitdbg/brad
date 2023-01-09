# Used to generate datasets for experiments
add_executable(dataset_admin
  column_gen.cc
  column_gen.h
  dataset_admin.cc
  dataset_admin.h
  dataset_admin_main.cc
)
target_link_libraries(dataset_admin PRIVATE cirrus gflags)
