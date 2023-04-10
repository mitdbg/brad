#! /bin/bash

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH
source shared.sh
cd ..

mode=$1
check_format="format"
check_lint="lint"
check_types="types"

if [ ! -z $mode ] && [ $mode != $check_format ] && [ $mode != $check_lint ] && [ $mode != $check_types ]; then
  echo "Usage $0 [format|lint|types]"
  exit 1
fi

set -e

echo_blue "Tooling"
echo_blue "======="
if [ -z $mode ] || [ $mode == $check_format ]; then
  black --version
fi
if [ -z $mode ] || [ $mode == $check_lint ]; then
  pylint --version
fi
if [ -z $mode ] || [ $mode == $check_types ]; then
  mypy --version
fi
echo ""

set +e

if [ -z $mode ] || [ $mode == $check_format ]; then
  echo_blue "Check Formatting (black)"
  echo_blue "========================"
  black --check .
  black_exit=$?
  echo ""
fi

if [ -z $mode ] || [ $mode == $check_lint ]; then
  echo_blue "Lint (pylint)"
  echo_blue "============="
  pylint src/brad/* setup.py tests
  pylint_exit=$?
  echo ""
fi

if [ -z $mode ] || [ $mode == $check_types ]; then
  echo_blue "Type Check (mypy)"
  echo_blue "================="
  MYPYPATH=src mypy -p brad -p tests
  mypy_exit=$?
  echo ""
fi

function report_status() {
  if [ -z $1 ]; then
    echo "- Skipped"
  elif [ $1 -eq 0 ]; then
    echo_green "✓ Passed"
  else
    echo_red "✗ Failed"
  fi
}

echo_blue "Results Summary"
echo_blue "==============="
echo -n "Formatting  "; report_status $black_exit
echo -n "Lint        "; report_status $pylint_exit
echo -n "Type Check  "; report_status $mypy_exit

if [ -z $mode ]; then
  if [ $black_exit -ne 0 ] || [ $pylint_exit -ne 0 ] || [ $mypy_exit -ne 0 ]; then
    exit 1
  fi
fi

if [ "$mode" == $check_format ]; then
  exit $black_exit
elif [ "$mode" == $check_lint ]; then
  exit $pylint_exit
elif [ "$mode" == $check_types ]; then
  exit $mypy_exit
fi
