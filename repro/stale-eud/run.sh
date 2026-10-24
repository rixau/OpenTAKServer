#!/usr/bin/env bash
# Reproduce stale EUD sessions against a local OpenTAKServer stack.
#
#   ./run.sh baseline        unmodified upstream/master
#   ./run.sh fixed           this checkout's working tree
#
# Environment knobs:
#   CHATS=40                 GeoChats sent to the reconnected EUD
#   GENERATIONS=4            connections for one uid: GENERATIONS-1 go silent, the last stays live
#   OTS_EUD_IDLE_TIMEOUT=60  passed to OTS (the fixed code's idle timeout; baseline ignores it)
#   SOAK=0                   seconds to keep a quiet, pinging client connected afterwards
#                            and check it is still there (use > the idle timeout)
#   IDLE_CHECK=1             also pause a client that never reconnects and time its handler's exit
#   KEEP=0                   leave the stack running at the end
#
# Nothing is published to the host. The compose project is "ots-stale-eud".
set -euo pipefail
cd "$(dirname "$0")"

MODE=${1:?usage: run.sh baseline|fixed}
CHATS=${CHATS:-40}
GENERATIONS=${GENERATIONS:-4}
IDLE=${OTS_EUD_IDLE_TIMEOUT:-60}
SOAK=${SOAK:-0}
IDLE_CHECK=${IDLE_CHECK:-1}
UID_=ANDROID-repro-0001
CALLSIGN=REPRO1
REPO=$(git rev-parse --show-toplevel)

case "$MODE" in
  baseline)
    rm -rf .work/baseline && mkdir -p .work/baseline
    git -C "$REPO" archive upstream/master opentakserver | tar -x -C .work/baseline
    CODE=$PWD/.work/baseline ;;
  fixed) CODE=$REPO ;;
  *) echo "unknown mode $MODE" >&2; exit 2 ;;
esac

printf 'OTS_CODE=%s\nOTS_EUD_IDLE_TIMEOUT=%s\n' "$CODE" "$IDLE" > .env
dc() { docker compose "$@"; }
log() { printf '\n=== %s\n' "$*"; }

clients() { docker ps -a --filter "name=^ots-repro-" --format '{{.Names}}'; }
cleanup_clients() {
  for c in $(clients); do
    docker unpause "$c" >/dev/null 2>&1 || true
    docker rm -f "$c" >/dev/null 2>&1 || true
  done
}

consumers() {
  # consumer count per queue for this EUD's uid and callsign queues
  local out
  out=$(dc exec -T rabbitmq rabbitmqctl list_consumers -q)
  printf '%s queue consumers: %s\n' "$UID_" "$(awk -v q="$UID_" '$1==q' <<<"$out" | wc -l)"
  printf '%s queue consumers: %s\n' "$CALLSIGN" "$(awk -v q="$CALLSIGN" '$1==q' <<<"$out" | wc -l)"
}

handlers() {
  # forked eud_handler children (one per connection) with their age in seconds
  { dc exec -T eud-handler-ssl ps -o pid=,etimes= --ppid 1 || true; } | awk '{$1=$1; print}'
}

listen() { # name uid callsign cert
  dc run -d --name "ots-repro-$1" client listen --ca /certs/ca.pem \
    --cert "/certs/$4.pem" --key "/certs/$4.key" --uid "$2" --callsign "$3" --name "$1" \
    --sa-interval 3600 \
    >/dev/null 2>&1
}

log "mode=$MODE code=$CODE idle_timeout=$IDLE"
cleanup_clients
dc down -v --remove-orphans >/dev/null 2>&1 || true
dc up -d --wait ots >/dev/null 2>&1
dc up -d cot-parser eud-handler-ssl >/dev/null 2>&1
until dc exec -T eud-handler-ssl grep -q "Started SSL server" /app/ots/logs/eud_handler_ssl.log 2>/dev/null; do sleep 1; done
sleep 3

log "provisioning users and certificates through the OTS API"
dc cp provision.py ots:/tmp/provision.py >/dev/null 2>&1
dc exec -T ots python /tmp/provision.py eud1 sender idle1
rm -rf .work/certs && mkdir -p .work/certs
dc cp ots:/app/ots/repro-certs/. .work/certs/ >/dev/null 2>&1
chmod a+r .work/certs/*

for g in $(seq 1 "$GENERATIONS"); do
  name=eud-g$g
  log "connection $g/$GENERATIONS for $UID_ ($name)"
  before=$(handlers | awk '{print $1}' | sort)
  listen "$name" "$UID_" "$CALLSIGN" eud1
  sleep 5
  after=$(handlers | awk '{print $1}' | sort)
  echo "new handler pid: $(comm -13 <(echo "$before") <(echo "$after") | tr '\n' ' ')"
  consumers
  if [ "$g" -lt "$GENERATIONS" ]; then
    docker pause "ots-repro-$name" >/dev/null
    echo "paused $name (socket left open, nothing sent or read)"
  fi
done

sleep 3
log "state before chats"
consumers
echo "eud_handler children (pid age_s):"; handlers

log "sending $CHATS GeoChats to $CALLSIGN"
dc run --rm client chat --ca /certs/ca.pem --cert /certs/sender.pem --key /certs/sender.key \
  --uid ANDROID-repro-sender --callsign SENDER --to-uid "$UID_" --to-callsign "$CALLSIGN" \
  --count "$CHATS" --tag "$MODE" >/dev/null 2>&1
sleep 3
live="ots-repro-eud-g$GENERATIONS"
got=$(docker logs "$live" 2>&1 | grep -c "RECEIVED $MODE-" || true)
log "RESULT: live client $live received $got/$CHATS chats"

if [ "$IDLE_CHECK" = 1 ]; then
  log "idle check: a client that goes silent and never reconnects"
  before=$(handlers | awk '{print $1}' | sort)
  listen idle1 ANDROID-repro-idle IDLE1 idle1
  sleep 5
  pid=$(comm -13 <(echo "$before") <(handlers | awk '{print $1}' | sort) | head -1)
  docker pause ots-repro-idle1 >/dev/null
  start=$(date +%s)
  echo "paused idle1, its handler is pid $pid; waiting up to $((IDLE + 60))s for it to exit"
  while handlers | awk '{print $1}' | grep -qx "$pid"; do
    if [ $(( $(date +%s) - start )) -gt $((IDLE + 60)) ]; then break; fi
    sleep 2
  done
  if handlers | awk '{print $1}' | grep -qx "$pid"; then
    log "RESULT: idle handler $pid still running after $(( $(date +%s) - start ))s"
  else
    log "RESULT: idle handler $pid exited $(( $(date +%s) - start ))s after its client went silent"
  fi
  out=$(dc exec -T rabbitmq rabbitmqctl list_consumers -q)
  echo "ANDROID-repro-idle queue consumers: $(awk '$1=="ANDROID-repro-idle"' <<<"$out" | wc -l)"
fi

if [ "$SOAK" -gt 0 ]; then
  log "soak: $live stays connected and quiet for ${SOAK}s, answering only with pings"
  sleep "$SOAK"
  if docker logs "$live" 2>&1 | grep -q "server closed\|socket error"; then
    log "RESULT: $live was disconnected during the soak"
  else
    pings=$(docker logs "$live" 2>&1 | grep -c " ping " || true)
    log "RESULT: $live still connected after ${SOAK}s soak ($pings pings sent in total)"
  fi
  consumers
fi

log "eud_handler log excerpts"
dc exec -T eud-handler-ssl grep -E "ERROR|Traceback|Error|disconnected|closing|another socket|Nothing received" \
  /app/ots/logs/eud_handler_ssl.log | tail -25 || true
echo "error lines in eud_handler log: $(dc exec -T eud-handler-ssl grep -c -E ' - ERROR - |Traceback' /app/ots/logs/eud_handler_ssl.log || true)"

if [ "${KEEP:-0}" != 1 ]; then
  cleanup_clients
  dc down -v >/dev/null 2>&1
fi
