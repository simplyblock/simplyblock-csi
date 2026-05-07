#!/usr/bin/env bash
# check-mtls.sh — verify mTLS enforcement across simplyblock components

set -euo pipefail

NAMESPACE="${NAMESPACE:-simplyblock}"
STORAGE_NAMESPACE="${STORAGE_NAMESPACE:-default}"
TLS_PATH="/etc/simplyblock/tls"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

pass() { echo -e "  ${GREEN}✔ PASS${RESET}  $1"; }
fail() { echo -e "  ${RED}✗ FAIL${RESET}  $1"; FAILURES=$((FAILURES + 1)); }
warn() { echo -e "  ${YELLOW}⚠ WARN${RESET}  $1"; }
info() { echo -e "  ${CYAN}ℹ${RESET}      $1"; }
header() { echo -e "\n${BOLD}=== $1 ===${RESET}"; }

FAILURES=0

# ── helpers ──────────────────────────────────────────────────────────────────

get_pod() {
    local ns="$1" prefix="$2"
    kubectl -n "$ns" get pods --no-headers 2>/dev/null \
        | awk -v p="$prefix" '$1 ~ "^"p && $3 == "Running" {print $1; exit}'
}

# Test mTLS enforcement on a server:
#   - WITHOUT client cert → expect TLS rejection (Request CERT + no HTTP response)
#   - WITH client cert    → expect HTTP response
# Args: test_pod test_ns test_container ca url cert key component_name
test_server_mtls() {
    local pod="$1" ns="$2" container="$3" ca="$4" url="$5" cert="$6" key="$7" name="$8"

    # Test 1: no client cert — should be rejected
    local out_no_cert
    out_no_cert=$(kubectl -n "$ns" exec "$pod" -c "$container" -- \
        curl -v --cacert "$ca" "$url" 2>&1 || true)

    local requested_cert http_response
    requested_cert=$(echo "$out_no_cert" | grep -c "Request CERT" || true)
    http_response=$(echo "$out_no_cert"  | grep -c "^< HTTP/"      || true)

    if [[ "$requested_cert" -gt 0 && "$http_response" -eq 0 ]]; then
        pass "$name: server requests client cert and rejects unauthenticated connections"
    elif [[ "$requested_cert" -gt 0 && "$http_response" -gt 0 ]]; then
        fail "$name: server requests client cert but DOES NOT reject — mTLS not enforced (CERT_OPTIONAL)"
    elif [[ "$http_response" -gt 0 ]]; then
        fail "$name: server does not request a client cert — plain TLS only, mTLS not enforced"
    else
        warn "$name: unexpected curl output — manual check needed"
        echo "$out_no_cert" | tail -5 | sed 's/^/          /'
    fi

    # Test 2: with client cert — should succeed
    local out_with_cert
    out_with_cert=$(kubectl -n "$ns" exec "$pod" -c "$container" -- \
        curl -v --cacert "$ca" --cert "$cert" --key "$key" "$url" 2>&1 || true)

    local http_ok
    http_ok=$(echo "$out_with_cert" | grep -c "^< HTTP/" || true)

    if [[ "$http_ok" -gt 0 ]]; then
        pass "$name: connection with valid client cert succeeds"
    else
        fail "$name: connection with valid client cert failed — check cert/CA setup"
        echo "$out_with_cert" | grep -E "error|alert|SSL" | head -3 | sed 's/^/          /'
    fi
}


# ── Resolve test pods ─────────────────────────────────────────────────────────

header "Resolving Test Pods"

OPERATOR_POD=$(get_pod "$NAMESPACE" "simplyblock-operator-")
SNODE_POD=$(get_pod "$STORAGE_NAMESPACE" "simplyblock-storage-node-ds-")
SPDK_SLICE=$(kubectl -n "$STORAGE_NAMESPACE" get endpointslices --no-headers 2>/dev/null \
    | grep spdk-proxy-endpoints | awk '{print $1}' | head -1)

[[ -n "$OPERATOR_POD" ]] && info "Operator pod:     $OPERATOR_POD" || warn "Operator pod not found"
[[ -n "$SNODE_POD"    ]] && info "Storage Node pod: $SNODE_POD"    || warn "Storage Node pod not found"

if [[ -n "$SPDK_SLICE" ]]; then
    SPDK_HOSTNAME=$(kubectl -n "$STORAGE_NAMESPACE" get endpointslice "$SPDK_SLICE" \
        -o jsonpath='{.endpoints[0].hostname}' 2>/dev/null)
    SPDK_PORT=$(kubectl -n "$STORAGE_NAMESPACE" get endpointslice "$SPDK_SLICE" \
        -o jsonpath='{.ports[0].port}' 2>/dev/null)
    SPDK_URL="https://${SPDK_HOSTNAME}.simplyblock-spdk-proxy.${STORAGE_NAMESPACE}.svc.cluster.local:${SPDK_PORT}/"
    info "SPDK Proxy URL:   $SPDK_URL"
else
    warn "No SPDK proxy endpoint slices found"
    SPDK_URL=""
fi

# ── mTLS enforcement ──────────────────────────────────────────────────────────

header "mTLS Enforcement"

# WebApp API — tested from operator pod
if [[ -n "$OPERATOR_POD" ]]; then
    info "Testing WebApp API..."
    test_server_mtls \
        "$OPERATOR_POD" "$NAMESPACE" "manager" \
        "$TLS_PATH/ca.crt" \
        "https://simplyblock-webappapi:5000/api/v1/health/fdb/" \
        "$TLS_PATH/tls.crt" "$TLS_PATH/tls.key" \
        "WebApp API"
else
    warn "WebApp API: skipped — no operator pod available"
fi

# Storage Node API — tested from operator pod
if [[ -n "$OPERATOR_POD" ]]; then
    SNODE_HOSTNAME=$(kubectl -n "$STORAGE_NAMESPACE" get endpointslice \
        -l "kubernetes.io/service-name=simplyblock-storage-node-api" \
        -o jsonpath='{.items[0].endpoints[0].hostname}' 2>/dev/null || true)
    if [[ -n "$SNODE_HOSTNAME" ]]; then
        SNODE_URL="https://${SNODE_HOSTNAME}.simplyblock-storage-node-api.${STORAGE_NAMESPACE}.svc.cluster.local:5000/snode/info"
    else
        SNODE_URL="https://simplyblock-storage-node-api.${STORAGE_NAMESPACE}.svc.cluster.local:5000/snode/info"
    fi
    info "Testing Storage Node API..."
    test_server_mtls \
        "$OPERATOR_POD" "$NAMESPACE" "manager" \
        "$TLS_PATH/ca.crt" \
        "$SNODE_URL" \
        "$TLS_PATH/tls.crt" "$TLS_PATH/tls.key" \
        "Storage Node API"
else
    warn "Storage Node API: skipped — no operator pod available"
fi

# SPDK Proxy — tested from operator pod
if [[ -n "$OPERATOR_POD" && -n "$SPDK_URL" ]]; then
    info "Testing SPDK Proxy..."
    test_server_mtls \
        "$OPERATOR_POD" "$NAMESPACE" "manager" \
        "$TLS_PATH/ca.crt" \
        "$SPDK_URL" \
        "$TLS_PATH/tls.crt" "$TLS_PATH/tls.key" \
        "SPDK Proxy"
else
    warn "SPDK Proxy: skipped — no operator pod or endpoint available"
fi



# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}════════════════════════════════════════${RESET}"
if [[ "$FAILURES" -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}  ALL CHECKS PASSED${RESET}"
else
    echo -e "${RED}${BOLD}  $FAILURES CHECK(S) FAILED${RESET}"
fi
echo -e "${BOLD}════════════════════════════════════════${RESET}"
echo ""

exit "$FAILURES"
