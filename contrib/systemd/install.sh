#!/bin/sh
set -eu

PREFIX=${PREFIX:-/usr/local}
SYSCONFDIR=${SYSCONFDIR:-/etc}
SYSTEMD_UNIT_DIR=${SYSTEMD_UNIT_DIR:-/etc/systemd/system}
DESTDIR=${DESTDIR:-}
BINARY=${BINARY:-target/release/legion}
ENABLE=${ENABLE:-0}

install_file() {
    mode=$1 source=$2 destination=$3
    install -D -m "$mode" "$source" "${DESTDIR}${destination}"
}

[ -f "$BINARY" ] || {
    echo "missing binary: $BINARY (run 'cargo build -p legion-server --release')" >&2
    exit 1
}

if [ -z "$DESTDIR" ]; then
    if ! getent group legion >/dev/null 2>&1; then
        groupadd --system legion
    fi
    if ! getent passwd legion >/dev/null 2>&1; then
        useradd --system --gid legion --home-dir /var/lib/legion --shell /usr/sbin/nologin legion
    fi
fi

install_file 0755 "$BINARY" "$PREFIX/bin/legion"
install_file 0644 contrib/systemd/legion.service "$SYSTEMD_UNIT_DIR/legion.service"

if [ ! -e "${DESTDIR}${SYSCONFDIR}/legion/legion.toml" ]; then
    install_file 0640 contrib/systemd/legion.toml "$SYSCONFDIR/legion/legion.toml"
fi
if [ ! -e "${DESTDIR}${SYSCONFDIR}/legion/legion.env" ]; then
    install_file 0600 contrib/systemd/legion.env "$SYSCONFDIR/legion/legion.env"
fi

if [ -z "$DESTDIR" ]; then
    chown root:legion "$SYSCONFDIR/legion/legion.toml" "$SYSCONFDIR/legion/legion.env"
fi

if [ -z "$DESTDIR" ] && [ "$ENABLE" = 1 ]; then
    systemctl daemon-reload
    systemctl enable --now legion.service
fi
