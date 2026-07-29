#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
DEPLOYMENT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly DEPLOYMENT_ROOT

# shellcheck disable=SC1091
source "${DEPLOYMENT_ROOT}/VERSIONS.lock"

case "$(uname -m)" in
x86_64 | amd64) ;;
*)
    printf 'error: just %s is pinned for x86_64 Linux\n' "${JUST_VERSION}" >&2
    exit 1
    ;;
esac

destination="${1:-/usr/local/bin}"
archive="just-${JUST_VERSION}-${JUST_TARGET}.tar.gz"
url="https://github.com/casey/just/releases/download/${JUST_VERSION}/${archive}"

if [[ -x "${destination}/just" ]] &&
    [[ "$("${destination}/just" --version)" == "just ${JUST_VERSION}" ]]; then
    printf 'just %s is already installed at %s\n' "${JUST_VERSION}" "${destination}/just"
    exit 0
fi

for command_name in curl sha256sum tar; do
    command -v "${command_name}" >/dev/null 2>&1 ||
        {
            printf 'error: required command not found: %s\n' "${command_name}" >&2
            exit 1
        }
done

temporary_dir="$(mktemp -d)"
trap 'rm -rf -- "${temporary_dir}"' EXIT

curl --fail --location --show-error --silent \
    "${url}" \
    --output "${temporary_dir}/${archive}"
printf '%s  %s\n' "${JUST_SHA256}" "${temporary_dir}/${archive}" |
    sha256sum --check
tar -xzf "${temporary_dir}/${archive}" -C "${temporary_dir}" just

if [[ ! -d "${destination}" ]]; then
    mkdir -p -- "${destination}" 2>/dev/null || true
fi

if [[ -d "${destination}" && -w "${destination}" ]]; then
    install -m 0755 "${temporary_dir}/just" "${destination}/just"
else
    command -v sudo >/dev/null 2>&1 ||
        {
            printf 'error: %s is not writable and sudo is unavailable\n' "${destination}" >&2
            exit 1
        }
    sudo install -d "${destination}"
    sudo install -m 0755 "${temporary_dir}/just" "${destination}/just"
fi

"${destination}/just" --version
