import os

filename = "pybind_brad_server.cpython-311-x86_64-linux-gnu.so"

# Assume we are running front end server from top-level directory `brad`
# src = f"./cpp/build/{filename}"
# TODO: Only absolute source path allows module to be imported in frontend?
src = f"/home/sophiez/brad/cpp/build/{filename}"
dst = f"./src/brad/native/{filename}"

try:
    os.symlink(src, dst)
except:
    # File already exists
    pass
