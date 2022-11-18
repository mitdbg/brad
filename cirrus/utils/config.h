#include <gflags/gflags.h>

// Redshift IAM role used for ETLs.
DECLARE_string(redshift_iam_role);

// Set this flag to print verbose output (useful for debugging).
DECLARE_bool(verbose);
