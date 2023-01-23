function pushd() {
    command pushd "$@" > /dev/null
}

function popd() {
    command popd "$@" > /dev/null
}

COLOR_RED="\033[0;31m"
COLOR_GREEN="\033[0;32m"
COLOR_YELLOW="\033[0;33m"
COLOR_BLUE="\033[0;36m"
COLOR_NC="\033[0m"

function echo_colored() {
  echo -e "${1}${2}${COLOR_NC}"
}

function echo_green() {
  echo_colored "$COLOR_GREEN" "$1"
}

function echo_red() {
  echo_colored "$COLOR_RED" "$1"
}

function echo_yellow() {
  echo_colored "$COLOR_YELLOW" "$1"
}

function echo_blue() {
  echo_colored "$COLOR_BLUE" "$1"
}

function prompt_yn() {
  echo -en "${COLOR_YELLOW}$1${COLOR_NC}"
  read -r
  if [[ ! $REPLY =~ ^[Yy]$ ]]
  then
    exit 1
  fi
}
