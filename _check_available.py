"""현재 실제 가용한 RunPod GPU 조회."""
import os, json, requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(".env.txt"))
KEY = os.environ.get("RUNPOD_API_KEY", "")
HDR = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}

res = requests.post(
    "https://api.runpod.io/graphql",
    json={"query": """{ gpuTypes {
      id displayName memoryInGb secureCloud communityCloud
      communityPrice securePrice
      lowestPrice { minimumBidPrice uninterruptablePrice }
    }}"""},
    headers=HDR, timeout=20,
).json()

gpus = (res.get("data") or {}).get("gpuTypes", [])

print(f"\n{'GPU 이름':<35} {'메모리':>6}  {'커뮤니티':>8}  {'시큐어':>8}  {'현재가용'}")
print("─" * 80)
for g in gpus:
    lp = g.get("lowestPrice") or {}
    available = "OK" if (lp.get("minimumBidPrice") or lp.get("uninterruptablePrice")) else "품절"
    cp = g.get("communityPrice") or 0
    sp = g.get("securePrice") or 0
    print(
        f"{g.get('displayName',''):<35} {g.get('memoryInGb',0):>5}GB"
        f"  ${cp:>6.3f}  ${sp:>6.3f}  {available}"
    )
