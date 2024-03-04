#! /bin/bash

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH
source shared.sh
cd ..

set -e

# We use --ci in our CI workflow to skip interactive prompts
if [ "$1" != "--ci" ]; then
  prompt_yn "Install BRAD in development mode? [y/N] "
fi

# Virtualenv is not used in our CI workflow
if [ "$1" == "--virtualenv" ]; then
  if [ ! -z $2 ]; then
    virtualenv -p $(which python3) $2
    source $2/bin/activate
  else
    echo "Usage: $0 [--virtualenv <venv name>]"
    exit 1
  fi
fi

is_venv=$(python3 -c "import sys; print(sys.prefix != sys.base_prefix)")

if [ $is_venv = "True" ]; then
  pip3 install --editable ".[dev,dashboard]"
else
  pip3 install --prefix $HOME/.local/ --editable ".[dev,dashboard]"
fi

echo_green "✓ Done"
