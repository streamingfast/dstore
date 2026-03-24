#!/bin/bash
# Single-node Ceph demo cluster for S3/RGW integration testing.
# Based on quay.io/ceph/ceph:v19 (native arm64 + amd64).
set -euo pipefail

log()  { echo "[$(date -u +%H:%M:%S)] $*"; }
die()  { log "ERROR: $*" >&2; exit 1; }
wait_for() {
    local desc="$1" timeout="$2"; shift 2
    for i in $(seq 1 "$timeout"); do
        if "$@" &>/dev/null 2>&1; then log "${desc} ready (${i}s)"; return 0; fi
        sleep 1
    done
    die "${desc} did not become ready after ${timeout}s"
}

# ── Config ───────────────────────────────────────────────────────────────────
CLUSTER="ceph"
MON_NAME=$(hostname -s)
MON_IP="127.0.0.1"
RGW_PORT="${RGW_PORT:-8080}"
DEMO_UID="${CEPH_DEMO_UID:-cephtest}"
DEMO_ACCESS_KEY="${CEPH_DEMO_ACCESS_KEY:-cephaccesskey}"
DEMO_SECRET_KEY="${CEPH_DEMO_SECRET_KEY:-cephsecretkey}"
DEMO_BUCKET="${CEPH_DEMO_BUCKET:-store-tests}"
OSD_SIZE_MB="${OSD_SIZE_MB:-1024}"

DATA="/var/lib/ceph"
INIT_DONE="${DATA}/.demo-initialized"

# ── First-time bootstrap ──────────────────────────────────────────────────────
if [[ ! -f "${INIT_DONE}" ]]; then
    log "Bootstrap: generating cluster config and keys..."

    FSID=$(python3 -c "import uuid; print(uuid.uuid4())")
    OSD_SIZE=$(( OSD_SIZE_MB * 1024 * 1024 ))

    mkdir -p \
        "${DATA}/mon/${CLUSTER}-${MON_NAME}" \
        "${DATA}/mgr/${CLUSTER}-${MON_NAME}" \
        "${DATA}/osd/${CLUSTER}-0" \
        "${DATA}/radosgw/${CLUSTER}.rgw0" \
        "${DATA}/bootstrap-osd" \
        /etc/ceph /var/run/ceph

    # ceph.conf
    cat > /etc/ceph/ceph.conf <<EOF
[global]
fsid                                  = ${FSID}
mon_initial_members                   = ${MON_NAME}
mon_host                              = ${MON_IP}
auth_cluster_required                 = cephx
auth_service_required                 = cephx
auth_client_required                  = cephx
osd_pool_default_size                 = 1
osd_pool_default_min_size             = 1
osd_crush_chooseleaf_type             = 0
mon_warn_on_insecure_global_id_reclaim_allowed = false
ms_warn_msgr2_not_enabled             = false
mon_allow_pool_delete                 = true

[osd.0]
osd_data                              = ${DATA}/osd/${CLUSTER}-0
osd_objectstore                       = bluestore
bluestore_block_create                = true
bluestore_block_size                  = ${OSD_SIZE}
EOF
    chown ceph:ceph /etc/ceph/ceph.conf

    # Keyrings
    ceph-authtool --create-keyring /tmp/mon.keyring \
        --gen-key -n mon. --cap mon 'allow *'
    ceph-authtool --create-keyring /etc/ceph/ceph.client.admin.keyring \
        --gen-key -n client.admin \
        --cap mon 'allow *' --cap osd 'allow *' \
        --cap mds 'allow *' --cap mgr 'allow *'
    ceph-authtool --create-keyring "${DATA}/bootstrap-osd/ceph.keyring" \
        --gen-key -n client.bootstrap-osd --cap mon 'profile bootstrap-osd'

    # Merge all keys into the mon keyring
    ceph-authtool /tmp/mon.keyring \
        --import-keyring /etc/ceph/ceph.client.admin.keyring
    ceph-authtool /tmp/mon.keyring \
        --import-keyring "${DATA}/bootstrap-osd/ceph.keyring"

    chown ceph:ceph /etc/ceph/ceph.client.admin.keyring \
        "${DATA}/bootstrap-osd/ceph.keyring"

    # Monitor map
    monmaptool --create --add "${MON_NAME}" "${MON_IP}" \
        --fsid "${FSID}" /tmp/monmap

    # Init monitor data dir
    ceph-mon --cluster "${CLUSTER}" --mkfs \
        -i "${MON_NAME}" --monmap /tmp/monmap --keyring /tmp/mon.keyring

    chown -R ceph:ceph "${DATA}/mon/${CLUSTER}-${MON_NAME}"
    touch "${INIT_DONE}"
    log "Bootstrap: done."
fi

# ── Monitor ───────────────────────────────────────────────────────────────────
log "Starting monitor..."
ceph-mon -f --cluster "${CLUSTER}" -i "${MON_NAME}" \
    --public-addr "${MON_IP}" \
    --setuser ceph --setgroup ceph &
MON_PID=$!

wait_for "Monitor" 60 ceph --connect-timeout 2 -s

# ── OSD ───────────────────────────────────────────────────────────────────────
OSD_DIR="${DATA}/osd/${CLUSTER}-0"

if [[ ! -f "${OSD_DIR}/ready" ]]; then
    log "Initializing OSD 0..."
    OSD_FSID=$(python3 -c "import uuid; print(uuid.uuid4())")

    OSD_ID=$(ceph osd new "${OSD_FSID}" 2>/dev/null)
    log "Allocated OSD id=${OSD_ID}"

    # The OSD id must be 0 for our ceph.conf [osd.0] section to match.
    # On a fresh cluster this will always be 0; guard just in case.
    [[ "${OSD_ID}" == "0" ]] || log "WARNING: OSD id=${OSD_ID} != 0; ceph.conf [osd.0] may not match"

    ceph auth get-or-create "osd.${OSD_ID}" \
        osd 'allow *' mon 'allow profile osd' mgr 'allow profile osd' \
        > "${OSD_DIR}/keyring"
    chown -R ceph:ceph "${OSD_DIR}"

    # mkfs — creates the block file via bluestore_block_create=true
    ceph-osd -i "${OSD_ID}" --mkfs --osd-uuid "${OSD_FSID}" \
        --setuser ceph --setgroup ceph
fi

log "Starting OSD..."
OSD_ID=$(cat "${OSD_DIR}/whoami" 2>/dev/null || echo 0)
ceph-osd -f -i "${OSD_ID}" --setuser ceph --setgroup ceph &
OSD_PID=$!

wait_for "OSD" 60 bash -c "ceph osd stat 2>/dev/null | grep -q '1 up'"

# ── Manager ───────────────────────────────────────────────────────────────────
log "Starting MGR..."
MGR_DIR="${DATA}/mgr/${CLUSTER}-${MON_NAME}"
if [[ ! -f "${MGR_DIR}/keyring" ]]; then
    ceph auth get-or-create "mgr.${MON_NAME}" \
        mon 'allow profile mgr' osd 'allow *' mds 'allow *' \
        > "${MGR_DIR}/keyring"
    chown -R ceph:ceph "${MGR_DIR}"
fi
ceph-mgr -f -i "${MON_NAME}" --setuser ceph --setgroup ceph &
MGR_PID=$!

# ── RGW realm / zone (first-time only) ───────────────────────────────────────
if ! radosgw-admin realm get --rgw-realm=default &>/dev/null 2>&1; then
    log "Setting up RGW realm/zone..."
    radosgw-admin realm    create --rgw-realm=default --default
    radosgw-admin zonegroup create --rgw-zonegroup=default --master --default
    radosgw-admin zone     create --rgw-zonegroup=default \
        --rgw-zone=default --master --default
    radosgw-admin period update --rgw-realm=default --commit
fi

# ── RGW ───────────────────────────────────────────────────────────────────────
RGW_DIR="${DATA}/radosgw/${CLUSTER}.rgw0"
if [[ ! -f "${RGW_DIR}/keyring" ]]; then
    ceph auth get-or-create "client.rgw.rgw0" \
        osd 'allow rwx' mon 'allow rw' mgr 'allow rw' \
        > "${RGW_DIR}/keyring"
    chown -R ceph:ceph "${RGW_DIR}"
fi

log "Starting RGW on port ${RGW_PORT}..."
radosgw -f \
    --rgw-frontends "beast port=${RGW_PORT}" \
    --name "client.rgw.rgw0" \
    --keyring "${RGW_DIR}/keyring" \
    --setuser ceph --setgroup ceph &
RGW_PID=$!

wait_for "RGW" 120 curl -sf --max-time 2 "http://localhost:${RGW_PORT}"

# ── User + bucket ─────────────────────────────────────────────────────────────
if ! radosgw-admin user info --uid="${DEMO_UID}" &>/dev/null 2>&1; then
    log "Creating RGW user '${DEMO_UID}'..."
    radosgw-admin user create \
        --uid="${DEMO_UID}" \
        --display-name="Ceph demo user" \
        --access-key="${DEMO_ACCESS_KEY}" \
        --secret-key="${DEMO_SECRET_KEY}"
    radosgw-admin caps add \
        --uid="${DEMO_UID}" \
        --caps="buckets=*;users=*;usage=*;metadata=*"
fi

if [[ -n "${DEMO_BUCKET}" ]]; then
    if ! radosgw-admin bucket stats --bucket="${DEMO_BUCKET}" &>/dev/null 2>&1; then
        log "Creating bucket '${DEMO_BUCKET}' via S3 API..."
        # radosgw-admin bucket create was removed in Ceph Reef+; use S3 API with
        # AWS Signature V2 via openssl (always present in the Ceph image).
        DATE=$(date -u +"%a, %d %b %Y %H:%M:%S +0000")
        SIG=$(printf "PUT\n\n\n%s\n/%s" "${DATE}" "${DEMO_BUCKET}" \
              | openssl sha1 -hmac "${DEMO_SECRET_KEY}" -binary | base64)
        RESP=$(curl -sf -o /dev/null -w "%{http_code}" \
            -X PUT \
            -H "Date: ${DATE}" \
            -H "Authorization: AWS ${DEMO_ACCESS_KEY}:${SIG}" \
            "http://localhost:${RGW_PORT}/${DEMO_BUCKET}" 2>&1) || true
        case "${RESP}" in
            200|204)   log "Bucket '${DEMO_BUCKET}' created (${RESP})" ;;
            409)       log "Bucket '${DEMO_BUCKET}' already exists" ;;
            *)         die "Bucket creation failed (HTTP ${RESP})" ;;
        esac
    fi
fi

log "============================================"
log " Ceph S3 demo cluster is ready"
log " Endpoint   : http://localhost:${RGW_PORT}"
log " Access key : ${DEMO_ACCESS_KEY}"
log " Secret key : ${DEMO_SECRET_KEY}"
log " Bucket     : ${DEMO_BUCKET}"
log "============================================"

# Keep container alive; exit if any daemon dies unexpectedly
wait -n
log "A daemon exited — shutting down"
kill "${MON_PID}" "${OSD_PID}" "${MGR_PID}" "${RGW_PID}" 2>/dev/null || true
