#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "$#" -ne 3 ]]; then
    echo "usage: $0 IMAGE EXPECTED_BINARY ABSENT_BINARY" >&2
    exit 2
fi

image="$1"
expected_binary="$2"
absent_binary="$3"
expected_entrypoint="[\"/usr/local/bin/${expected_binary}\"]"

actual_user="$(docker image inspect "${image}" --format '{{.Config.User}}')"
actual_entrypoint="$(docker image inspect "${image}" --format '{{json .Config.Entrypoint}}')"
actual_stop_signal="$(docker image inspect "${image}" --format '{{.Config.StopSignal}}')"

[[ "${actual_user}" == "10001:10001" ]] || {
    echo "unexpected image user: ${actual_user}" >&2
    exit 1
}
[[ "${actual_entrypoint}" == "${expected_entrypoint}" ]] || {
    echo "unexpected entrypoint: ${actual_entrypoint}" >&2
    exit 1
}
[[ "${actual_stop_signal}" == "SIGTERM" ]] || {
    echo "unexpected stop signal: ${actual_stop_signal}" >&2
    exit 1
}

docker run --rm --read-only --network none "${image}" --help >/dev/null
docker run \
    --rm \
    --read-only \
    --network none \
    --entrypoint /bin/sh \
    "${image}" \
    -eu -c \
    "test -x /usr/local/bin/${expected_binary}; test ! -e /usr/local/bin/${absent_binary}"
