"""Create the reproduction's users and client certificates through OTS's own API.

Runs inside the ots container (it needs localhost:8081 and the CA folder), as
the administrator account OTS creates on first start.
"""

import os
import shutil
import sys

import requests

BASE = "http://localhost:8081"
USERS = sys.argv[1:] or ["eud1", "sender"]
OUT = "/app/ots/repro-certs"
CA = "/app/ots/ca"

s = requests.Session()
s.headers["Accept"] = "application/json"
csrf = s.get(f"{BASE}/api/login", json={}).json()["response"]["csrf_token"]
s.headers["X-CSRFToken"] = csrf
r = s.post(f"{BASE}/api/login", json={"username": "administrator", "password": "password"})
r.raise_for_status()

os.makedirs(OUT, exist_ok=True)
shutil.copy(f"{CA}/ca.pem", f"{OUT}/ca.pem")
for user in USERS:
    pw = "Repro-Passw0rd!"
    r = s.post(
        f"{BASE}/api/user/add",
        json={"username": user, "password": pw, "confirm_password": pw, "roles": ["user"]},
    )
    print("add", user, r.status_code, r.text.strip()[:120])
    r = s.post(f"{BASE}/api/certificate", json={"username": user})
    print("cert", user, r.status_code, r.text.strip()[:120])
    d = f"{CA}/certs/{user}"
    shutil.copy(f"{d}/{user}.pem", f"{OUT}/{user}.pem")
    shutil.copy(f"{d}/{user}.nopass.key", f"{OUT}/{user}.key")
