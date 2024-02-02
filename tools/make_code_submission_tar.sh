#! /bin/bash

# This script is used to package up our code for submission.

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH
source shared.sh
cd ..

mkdir brad
cp -r src brad
cp setup.py brad
tar czf brad_code.tar.gz brad
rm -r brad
