#!/usr/bin/env bash

# This script triggers a single experiment by calling the endpoint provided by the Antithesis team.
#
# This script takes 5 arguments:
#  1. Workload (test) name
#  2. Duration (wallclock)
#  3. Images tag
#  4. Experiment description (empty string is ok)
#  5. Additional email addresses to send the report to (empty string is ok)
# Reference:
#  - https://antithesis.com/docs/getting_started/webhook.html

################################################################################

# URL
URL='https://synadia.antithesis.com/api/v1/launch_experiment/synadia'
REQUIRED="
    jq
    curl
"

################################################################################

# Ensure required commands are available
for cmd in ${REQUIRED}; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        echo "Missing required command: ${cmd}"
        exit 1
    fi
done

# Ensure API username is set in environment
if [ -z "${ANTITHESIS_WEBOOK_USERNAME}" ]; then
    echo "Environment variable ANTITHESIS_WEBOOK_USERNAME is not set."
    exit 1
fi

# Ensure token is set in environment
if [ -z "${ANTITHESIS_WEBOOK_TOKEN}" ]; then
    echo "Environment variable ANTITHESIS_WEBOOK_TOKEN is not set."
    exit 1
fi

# Ensure URL is set in environment
if [ -z "${ANTITHESIS_WEBOOK_URL}" ]; then
    echo "Environment variable ANTITHESIS_WEBOOK_TOKEN is not set."
    exit 1
fi
URL="${ANTITHESIS_WEBOOK_URL}"


# Ensure the right number of arguments
if [ "$#" -ne 5 ]; then
    echo "Expecting 5 arguments, got $#: ${*}"
    exit 1
fi

# Put arguments into variables for readability
workload="${1}"
duration="${2}"
images_tag="${3}"
experiment_description="[${GITHUB_ACTOR:-$USER}] ${4}"
email="antithesis-reports@synadia.com;${5}"

echo "Triggering ${duration} hours experiment with workload: '${workload}' using images version: '${images_tag}'"

################################################################################

# Don't ignore mid-pipeline errors
set -o pipefail

# Invoke endpoint
jq -n \
    --arg w "${workload}" \
    --arg d "${duration}" \
    --arg i "raft_sut:${images_tag};raft_workload:${images_tag};raft_config:${images_tag};nats_server:latest" \
    --arg n "${experiment_description}" \
    --arg e "${email}" \
    '{ params: {"custom.workload": $w, "custom.duration": $d, "antithesis.images": $i, "antithesis.description": $n, "antithesis.report.recipients": $e } }' \
| tee /dev/fd/2 \
| curl \
    --fail \
    --silent \
    --show-error \
    -u "${ANTITHESIS_WEBOOK_USERNAME}:${ANTITHESIS_WEBOOK_TOKEN}" \
    -X POST \
    --data @- \
    -H "content-type: application/json" \
    "${URL}"

if [ $? -ne 0 ]; then
    echo "Error launching experiment"
    exit 1
fi
