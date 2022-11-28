#include <gflags/gflags.h>

DEFINE_string(redshift_iam_role, "", "Redshift IAM role used for ETLs.");
DEFINE_bool(verbose, false,
            "Set this flag to print verbose output (useful for debugging).");
