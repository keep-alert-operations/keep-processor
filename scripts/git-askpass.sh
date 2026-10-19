#!/bin/sh
case "$1" in
  *Username*) printf '%s\n' "${APPCHI_GIT_USERNAME:-x-access-token}" ;;
  *Password*) cat "${APPCHI_GIT_TOKEN_FILE:-/run/secrets/github_token}" ;;
  *) exit 1 ;;
esac
