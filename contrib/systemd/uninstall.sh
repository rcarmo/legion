#!/bin/sh
set -eu

PREFIX=${PREFIX:-/usr/local}
SYSCONFDIR=${SYSCONFDIR:-/etc}
SYSTEMD_UNIT_DIR=${SYSTEMD_UNIT_DIR:-/etc/systemd/system}
DESTDIR=${DESTDIR:-}

if [ -z "$DESTDIR" ]; then
    systemctl disable --now legion.service 2>/dev/null || true
fi

rm -f "${DESTDIR}${PREFIX}/bin/legion"
rm -f "${DESTDIR}${SYSTEMD_UNIT_DIR}/legion.service"

if [ -z "$DESTDIR" ]; then
    systemctl daemon-reload
fi

cat <<EOF
Legion binaries and service unit removed.
Preserved configuration: ${DESTDIR}${SYSCONFDIR}/legion
Preserved state: ${DESTDIR}/var/lib/legion
EOF
