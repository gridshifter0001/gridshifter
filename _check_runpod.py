import os, requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(".env.txt"))
key = os.environ.get("RUNPOD_API_KEY", "")

query = """{ gpuTypes {
  id displayName memoryInGb
  secureCloud communityCloud
  securePrice communityPrice communitySpotPrice
}}"""

r = requests.post(
    "https://api.runpod.io/graphql",
    json={"query": query},
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    timeout=15,
)
gpu_types = r.json().get("data", {}).get("gpuTypes", [])

print(f"{'ID':<45} {'이름':<30} {'커뮤니티':<10} {'시큐어':<10}")
print("-" * 100)
for g in gpu_types:
    if g.get("communityPrice") or g.get("securePrice"):
        print(
            f"{g['id']:<45} {g.get('displayName',''):<30} "
            f"${g.get('communityPrice') or 0:.3f}     "
            f"${g.get('securePrice') or 0:.3f}"
        )
