#include <gflags/gflags.h>

#include <iostream>

#include "dataset_admin.h"

DEFINE_string(action, "generate", "What to do. One of {generate, load}.");

DEFINE_string(config, "", "Path to the dataset configuration file.");
DEFINE_uint32(sf, 1, "The scale factor to use.");

DEFINE_string(out_path, "", "Where to output the generated files.");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Handles generating and loading data.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  cirrus::DatasetAdmin dataset(FLAGS_config, FLAGS_sf);

  if (FLAGS_action == "generate") {
    dataset.GenerateTo(FLAGS_out_path);
  } else if (FLAGS_action == "load") {
  } else {
    std::cerr << "ERROR: Unrecognized action " << FLAGS_action << std::endl;
    return 1;
  }

  return 0;
}
