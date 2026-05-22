"""RunPod pod 생성 API 응답 디버그."""
import os, json, requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(".env.txt"))
KEY = os.environ.get("RUNPOD_API_KEY", "")
URL = "https://api.runpod.io/graphql"
HDR = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}

def gql(query, variables=None):
    body = {"query": query}
    if variables:
        body["variables"] = variables
    r = requests.post(URL, json=body, headers=HDR, timeout=20)
    print(f"HTTP {r.status_code}")
    return r.json()

# ── 1. 계정 잔액 확인 ──────────────────────────────────────────────
print("\n=== 잔액 확인 ===")
res = gql("{ myself { currentSpendPerHr spendLimit } }")
print(json.dumps(res, indent=2, ensure_ascii=False))

# ── 2. L40 가용 여부 확인 ──────────────────────────────────────────
print("\n=== L40 가용 여부 ===")
res = gql("""{ gpuTypes { id displayName secureCloud communityCloud
  lowestPrice { minimumBidPrice uninterruptablePrice } } }""")
for g in (res.get("data") or {}).get("gpuTypes", []):
    if "L40" in g.get("displayName", ""):
        print(json.dumps(g, indent=2, ensure_ascii=False))

# ── 3. Pod 생성 실제 시도 (raw 응답 확인) ─────────────────────────
print("\n=== Pod 생성 시도 (NVIDIA L40) ===")
mutation = """
mutation($input: PodFindAndDeployOnDemandInput!) {
  podFindAndDeployOnDemand(input: $input) {
    id name imageName desiredStatus
  }
}"""
variables = {"input": {
    "cloudType": "ALL",
    "gpuCount": 1,
    "volumeInGb": 0,
    "containerDiskInGb": 10,
    "minMemoryInGb": 10,
    "gpuTypeId": "NVIDIA L40",
    "name": "gs-debug-test",
    "imageName": "pytorch/pytorch:latest",
    "dockerArgs": "",
    "ports": "22/tcp",
    "volumeMountPath": "/workspace",
    "env": [],
}}
res = gql(mutation, variables)
print(json.dumps(res, indent=2, ensure_ascii=False))
