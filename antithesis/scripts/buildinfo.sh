#!/usr/bin/env bash

# This script is invoked by GH workflows to populate the version file in each image

set -e

SUT_DIR="./antithesis/system-under-test"
WORKLOAD_DIR="./antithesis/workload"
CONFIG_DIR="./antithesis/config"

VERSION_FILE="version"
BUILDINFO_FILE="buildinfo"

for d in "${SUT_DIR}" "${WORKLOAD_DIR}" "${CONFIG_DIR}"; do
  # Files are expected to exist
  test -f "${d}/${VERSION_FILE}"
  test -f "${d}/${BUILDINFO_FILE}"

  # Write branch name and commit info into each image 'version' file
  echo "Branch: $(git branch --show-current)" > "${d}/${VERSION_FILE}"
  git log -1 >> "${d}/${VERSION_FILE}"

  # Write build metadata into each image 'buildinfo' file
  echo "
GITHUB_ACTOR=${GITHUB_ACTOR}
GITHUB_TRIGGERING_ACTOR=${GITHUB_TRIGGERING_ACTOR}
GITHUB_REF_NAME=${GITHUB_REF_NAME}
GITHUB_SHA=${GITHUB_SHA}
GITHUB_JOB=${GITHUB_JOB}
GITHUB_REPOSITORY=${GITHUB_REPOSITORY}
GITHUB_RUN_ID=${GITHUB_RUN_ID}
GITHUB_RUN_NUMBER=${GITHUB_RUN_NUMBER}
GITHUB_WORKFLOW=${GITHUB_WORKFLOW}
GITHUB_WORKFLOW_REF=${GITHUB_WORKFLOW_REF}
RUNNER_ARCH=${RUNNER_ARCH}
RUNNER_OS=${RUNNER_OS}
USER=${USER}
HOSTNAME=${HOSTNAME}
" > "${d}/${BUILDINFO_FILE}"

done
