#!/usr/bin/env bash
# Shared by the end-to-end script tests: a disposable copy of the deployment
# with a synthetic .env and a docker stub on PATH. Source it after lib.sh.

# Copies the deployment's files into ROOT/ecash with LOCK as its VERSIONS.lock.
# The copy has no git history, as when the deployment is unpacked on a host.
make_deployment_copy() {
    local root="$1" lock="$2"
    local copy="${root}/ecash"
    mkdir -p -- "${copy}" "${root}/bin"
    git -C "${DEPLOYMENT_ROOT}" ls-files -z --cached --others --exclude-standard . |
        tar --directory="${DEPLOYMENT_ROOT}" --null --files-from=- --create --file=- |
        tar --extract --file=- --directory="${copy}"
    cp -- "${lock}" "${copy}/VERSIONS.lock"
    sed -e "s/^PUID=.*/PUID=$(id -u)/" -e "s/^PGID=.*/PGID=$(id -g)/" \
        "${DEPLOYMENT_ROOT}/.env.example" >"${copy}/.env"

    # STUB_PS sets what `compose ps` answers: empty, running or fail, and
    # STUB_IMAGE the image `docker inspect` reports for a container. Rendering
    # goes to the real Docker CLI when there is one; it needs no daemon.
    cat >"${root}/bin/docker" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"${root}/docker.log"
case " \$* " in
*" compose "*" config "*)
    if [[ -n "${REAL_DOCKER:-}" ]]; then exec "${REAL_DOCKER:-}" "\$@"; fi ;;
*" compose "*" ps "*)
    case "\${STUB_PS:-empty}" in
    fail) printf 'Cannot connect to the Docker daemon\n' >&2; exit 1 ;;
    running) printf 'c0ffee\n' ;;
    esac ;;
*" inspect "*)
    printf '%s\n' "\${STUB_IMAGE:-}" ;;
esac
exit 0
EOF
    chmod +x -- "${root}/bin/docker"
    printf '%s\n' "${copy}"
}

REAL_DOCKER="$(command -v docker || true)"
