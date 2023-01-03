add_library(ycsbr STATIC zipfian_chooser.cc)
target_include_directories(ycsbr PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
