"""RunPod pod 생성 파라미터 조합 테스트."""
import os, json, requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(".env.txt"))
KEY = os.environ.get("RUNPOD_API_KEY", "")
HDR = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
URL = "https://api.runpod.io/graphql"

MUTATION = """
mutation($input: PodFindAndDeployOnDemandInput!) {
  podFindAndDeployOnDemand(input: $input) {
    id name imageName desiredStatus
    machine { podHostId }
  }
}"""

# 시도할 GPU 순서 (저렴한 것부터)
CANDIDATES = [
    ("NVIDIA GeForce RTX 4090",  "RTX 4090  $0.34/hr"),
    ("NVIDIA RTX A5000",         "RTX A5000 $0.16/hr"),
    ("NVIDIA A40",               "A40       $0.35/hr"),
    ("NVIDIA L40",               "L40       $0.69/hr"),
    ("NVIDIA L40S",              "L40S      $0.79/hr"),
    ("NVIDIA A100 80GB PCIe",    "A100 PCIe $1.19/hr"),
]

for gpu_id, label in CANDIDATES:
    print(f"\n[시도] {label}  (ID: {gpu_id})")
    variables = {"input": {
        "cloudType":        "ALL",
        "gpuCount":         1,
        "volumeInGb":       0,
        "containerDiskInGb": 10,
        "gpuTypeId":        gpu_id,
        "name":             "gs-test-pod",
        "imageName":        "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel-ubuntu22.04",
        "dockerArgs":       "",
        "ports":            "",
        "volumeMountPath":  "/workspace",
        "env":              [],
    }}
    r = requests.post(URL, json={"query": MUTATION, "variables": variables}, headers=HDR, timeout=20)
    data = r.json()

    if "errors" in data:
        code = data["errors"][0].get("extensions", {}).get("code", "?")
        msg  = data["errors"][0]["message"][:80]
        print(f"  FAIL [{code}] {msg}")
    else:
        pod = (data.get("data") or {}).get("podFindAndDeployOnDemand")
        if pod and pod.get("id"):
            print(f"  SUCCESS! Pod ID: {pod['id']}")
            print(f"  즉시 종료 처리 중...")
            # 바로 종료
            term = """mutation($input: PodTerminateInput!) { podTerminate(input: $input) }"""
            requests.post(URL, json={"query": term, "variables": {"input": {"podId": pod["id"]}}}, headers=HDR, timeout=10)
            print(f"  Pod 종료 완료. 이 GPU 타입 사용 가능!")
            break
        else:
            print(f"  FAIL 응답에 ID 없음: {json.dumps(data)[:120]}")
