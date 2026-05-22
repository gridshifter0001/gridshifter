# GridShifter AI

H100 12대를 서울 리전에서 8시간 돌리면 약 $1,296 나옵니다. GridShifter는 전 세계 전력망 가격과 클라우드 GPU 재고를 실시간으로 비교해서 같은 작업을 더 싸게 할 수 있는 리전을 추천합니다. 텍사스 야간 풍력 잉여 시간대 + RunPod 유럽 리전 조합 시 $389까지 내려간 사례가 있습니다.

**[대시보드 →](https://gridshifter.app)**

---

## 어떻게 동작하나요

AWS, Lambda Labs, RunPod의 스팟 가격과 재고를 긁어오고, ERCOT(텍사스)·KEPCO(한국)·ENTSO-E(유럽) 전력 도매가와 조합합니다. 단순히 가장 싼 곳을 추천하는 게 아니라 재고 부족 리스크와 스팟 회수 확률을 함께 고려한 SSS 점수로 랭킹을 매깁니다.

LIVE 서비스(인바운드 포트 열린 경우)는 지연 80ms 이상 리전을 자동으로 배제하고, 학습 배치 잡은 지연 제약 없이 글로벌 전체를 탐색합니다.

## 설치 및 사용

```bash
pip install -r requirements.txt
cp .env.example .env.txt   # API 키 입력
```

대시보드는 브라우저에서 바로 쓸 수 있고, CLI로 실제 인스턴스 배포까지 가능합니다.

```bash
# 추천 리전 확인만 (실제 배포 없음)
python gridshifter.py run --image pytorch/pytorch --gpu H100 --count 4 --hours 8 --dry-run

# 추천 리전에 바로 배포 + 8시간 후 자동 종료
python gridshifter.py run --image myrepo/train:latest --gpu L40 --count 2 --hours 8 --auto-terminate
```

API 키가 없으면 시뮬레이션 데이터로 대체됩니다. AWS/Lambda/RunPod 키가 있을 때 실시간 데이터로 전환됩니다.

## API

FastAPI로 구동되며 Swagger 문서는 `/docs`에서 확인할 수 있습니다.

```
GET  /api/v1/quick-optimize?gpu_count=4&job_hours=8&workload=batch
POST /api/v1/optimize
```

라이브 API: `https://api.gridshifter.app`

## 스택

Python · FastAPI · NumPy · boto3 · Railway · GitHub Pages

## 주의

CLI 배포는 실제 과금이 발생합니다. `--dry-run`으로 먼저 확인하세요. 스팟 인스턴스는 공급자 사정으로 강제 회수될 수 있습니다.
