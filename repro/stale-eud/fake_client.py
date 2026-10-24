"""A minimal TAK streaming client for reproducing stale EUD sessions.

It connects to the SSL streaming port with a client certificate, announces a
fixed uid and callsign with a contact/takv SA event, and then behaves like
ATAK's commo library: it pings (t-x-c-t) after RX_STALE seconds without hearing
from the server, repeats the ping every 4.5 seconds while the silence lasts, and
re-sends its SA every --sa-interval seconds.

Modes:
  listen  stay connected and print every GeoChat it receives
  chat    send --count GeoChats to --to-callsign / --to-uid, then exit

Standard library only, so it runs in a bare python image.
"""

import argparse
import datetime
import socket
import ssl
import sys
import time
import uuid

RX_STALE = 15.0
RX_STALE_PING = 4.5


def now_iso(offset=0):
    t = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=offset)
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sa(uid, callsign):
    return (
        f'<event version="2.0" uid="{uid}" type="a-f-G-U-C" how="m-g" time="{now_iso()}" '
        f'start="{now_iso()}" stale="{now_iso(300)}">'
        '<point lat="38.8977" lon="-77.0365" hae="10" ce="10" le="10"/>'
        f'<detail><takv device="repro" platform="ATAK-CIV" os="34" version="5.4.0"/>'
        f'<contact callsign="{callsign}" endpoint="*:-1:stcp"/>'
        '<__group name="Cyan" role="Team Member"/>'
        f'<uid Droid="{callsign}"/></detail></event>'
    )


def ping(uid):
    return (
        f'<event version="2.0" uid="{uid}-ping" type="t-x-c-t" how="m-g" time="{now_iso()}" '
        f'start="{now_iso()}" stale="{now_iso(10)}">'
        '<point lat="0" lon="0" hae="0" ce="9999999" le="9999999"/><detail/></event>'
    )


def geochat(uid, callsign, to_uid, to_callsign, text):
    msg_id = str(uuid.uuid4())
    return (
        f'<event version="2.0" uid="GeoChat.{uid}.{to_uid}.{msg_id}" type="b-t-f" how="h-g-i-g-o" '
        f'time="{now_iso()}" start="{now_iso()}" stale="{now_iso(86400)}">'
        '<point lat="0" lon="0" hae="0" ce="9999999" le="9999999"/><detail>'
        f'<__chat parent="RootContactGroup" groupOwner="false" messageId="{msg_id}" '
        f'chatroom="{to_callsign}" id="{to_uid}" senderCallsign="{callsign}">'
        f'<chatgrp uid0="{uid}" uid1="{to_uid}" id="{to_uid}"/></__chat>'
        f'<link uid="{uid}" type="a-f-G-U-C" relation="p-p"/>'
        f'<remarks source="BAO.F.ATAK.{uid}" to="{to_uid}" time="{now_iso()}">{text}</remarks>'
        f'<marti><dest callsign="{to_callsign}"/></marti></detail></event>'
    )


def log(name, msg):
    print(f"[{now_iso()}] {name}: {msg}", flush=True)


def connect(args):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.load_verify_locations(args.ca)
    ctx.load_cert_chain(args.cert, args.key)
    raw = socket.create_connection((args.host, args.port), timeout=10)
    sock = ctx.wrap_socket(raw)
    sock.settimeout(1.0)
    return sock


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["listen", "chat"])
    p.add_argument("--host", default="eud-handler-ssl")
    p.add_argument("--port", type=int, default=8089)
    p.add_argument("--ca", required=True)
    p.add_argument("--cert", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--uid", required=True)
    p.add_argument("--callsign", required=True)
    p.add_argument("--name", default=None)
    p.add_argument("--sa-interval", type=float, default=60)
    p.add_argument("--to-uid")
    p.add_argument("--to-callsign")
    p.add_argument("--count", type=int, default=20)
    p.add_argument("--tag", default="msg")
    p.add_argument("--gap", type=float, default=0.2)
    args = p.parse_args()
    name = args.name or args.callsign

    sock = connect(args)
    log(name, f"connected {sock.getsockname()} -> {sock.getpeername()}")
    sock.sendall(sa(args.uid, args.callsign).encode())
    last_sa = time.time()

    if args.mode == "chat":
        time.sleep(3)  # let the handler identify us before we start talking
        for i in range(1, args.count + 1):
            text = f"{args.tag}-{i:03d}"
            sock.sendall(
                geochat(args.uid, args.callsign, args.to_uid, args.to_callsign, text).encode()
            )
            log(name, f"SENT {text}")
            time.sleep(args.gap)
        time.sleep(3)
        sock.close()
        return

    buf = ""
    last_rx = time.time()
    next_ping = 0.0
    pings = 0
    while True:
        try:
            data = sock.recv(65536)
            if not data:
                log(name, "server closed the connection")
                return 1
            last_rx = time.time()
            buf += data.decode("utf-8", "replace")
            while "</event>" in buf:
                event, buf = buf.split("</event>", 1)
                if 'type="b-t-f"' in event and "<remarks" in event:
                    text = event.split("<remarks", 1)[1].split(">", 1)[1].split("</remarks", 1)[0]
                    log(name, f"RECEIVED {text}")
                elif "t-x-c-t" in event:
                    log(name, "pong")
        except socket.timeout:
            pass
        except OSError as e:
            log(name, f"socket error: {e}")
            return 1

        now = time.time()
        if now - last_rx > RX_STALE and now > next_ping:
            sock.sendall(ping(args.uid).encode())
            pings += 1
            log(name, f"ping {pings}")
            next_ping = now + RX_STALE_PING
        if now - last_sa > args.sa_interval:
            sock.sendall(sa(args.uid, args.callsign).encode())
            last_sa = now


if __name__ == "__main__":
    sys.exit(main())
