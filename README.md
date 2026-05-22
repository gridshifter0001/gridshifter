# ⚡ GridShifter AI

> **글로벌 전력망 가격 + 클라우드 GPU 재고를 실시간 매칭해 AI 학습 비용을 최대 70% 절감하는 인프라 최적화 도구**

[![Live Dashboard](https://img.shields.io/badge/Dashboard-Live-22c55e?style=flat-square)](https://gridshifter0001.github.io/gridshifter/)
[![API](https://img.shields.io/badge/API-Railway-7c3aed?style=flat-square)](https://gridshifter-production.up.railway.app/api/v1/health)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)

---

## 🌐 라이브 데모

| 서비스 | URL |
|---|---|
| 대시보드 | https://gridshifter0001.github.io/gridshifter/ |
| API 헬스체크 | https://gridshifter-production.up.railway.app/api/v1/health |
| API 문서 (Swagger) | https://gridshifter-production.up.railway.app/docs |

---

## 💡 무엇을 해결하나요?

AI 모델 학습에 H100 GPU 12대를 8시간 돌리면 서울 리전 기준 약 **$1,296** 이 청구됩니다.

GridShifter AI는 다음 두 가지를 동시에 분석해 최적 리전을 추천합니다.

- **전력망 가격**: 한국(KEPCO), 텍사스(ERCOT), 유럽(ENTSO-E) 실시간 도매가
- **GPU 재고**: AWS, Lambda Labs, RunPod 전 리전 스팟 가격 및 가용 재고

> 텍사스 야간 풍력 잉여 전력 시간대 + RunPod 유럽 리전 조합 시 동일 작업 **$389** 달성 예시

---

## 🏗️ 아키텍처

```
┌─────────────────────────────────────────────────────┐
│  index.html (GitHub Pages)                          │
│  대시보드 · GPU 수량 · 시간 · 워크로드 타입 입력       │
└────────────────────┬────────────────────────────────┘
                     │ HTTPS
┌────────────────────▼────────────────────────────────┐
│  FastAPI (Railway)                                  │
│  POST /api/v1/optimize                              │
│  GET  /api/v1/quick-optimize                        │
└────────┬─────────────────────┬───────────────────────┘
         │                     │
┌────────▼──────┐   ┌──────────▼──────────────────────┐
│ PowerGrid     │   │ CloudTelemetry                  │
│ KEPCO  (KR)   │   │ AWS Spot     (전 리전)           │
│ ERCOT  (TX)   │   │ Lambda Labs  (전 리전)           │
│ ENTSO-E (EU)  │   │ RunPod       (전 리전)           │
└───────────────┘   └─────────────────────────────────┘
         │                     │
┌────────▼─────────────────────▼───────────────────────┐
│  SSS (Stability-Saving Score) Solver                 │
│  SSS = w1·절감률 − w2·재고리스크 − w3·회수확률        │
│  + 컴플라이언스 · 지연 · Cross-CSP 가드레일           │
└──────────────────────────────────────────────────────┘
```

---

## 🚀 빠른 시작

### 대시보드 사용 (브라우저)

1. https://gridshifter0001.github.io/gridshifter/ 접속
2. GPU 수량, 작업 시간, 베이스라인 리전 설정
3. **최적화 실행** 클릭
4. SSS 점수 기준 전 세계 리전 비교 결과 확인

### CLI 사용 (로컬)

```bash
# 설치
pip install -r requirements.txt

# 환경변수 설정 (.env.txt 또는 시스템 환경변수)
cp .env.example .env.txt
# .env.txt 에 API 키 입력

# 드라이런 (실제 배포 없이 추천 리전 확인)
python gridshifter.py run \
  --image pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime \
  --gpu H100 \
  --count 4 \
  --hours 8 \
  --dry-run

# 실제 배포 (RunPod 또는 Lambda Labs 자동 선택)
python gridshifter.py run \
  --image myrepo/train:latest \
  --gpu L40 \
  --count 2 \
  --hours 4 \
  --auto-terminate

# 상태 확인
python gridshifter.py status
```

### CLI 주요 옵션

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--image` | (필수) | Docker 이미지 |
| `--gpu` | `L40` | GPU 모델 (H100/A100/L40/A6000 등) |
| `--count` | `1` | GPU 수량 |
| `--hours` | `8` | 작업 예상 시간 |
| `--dry-run` | off | 추천만 확인, 실제 배포 안 함 |
| `--auto-terminate` | off | `--hours` 경과 후 인스턴스 자동 삭제 |
| `--save` | off | 배포 결과를 JSON으로 저장 |

---

## 🔑 환경변수

`.env.example` 파일 참고. 아래 키가 필요합니다.

| 변수 | 용도 | 필수 여부 |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | AWS 스팟 가격 조회 | 권장 |
| `AWS_SECRET_ACCESS_KEY` | AWS 인증 | 권장 |
| `LAMBDA_LABS_API_KEY` | Lambda Labs 재고 | 권장 |
| `RUNPOD_API_KEY` | RunPod 재고 및 배포 | 권장 |
| `ENTSOE_API_KEY` | 유럽 전력 가격 | 선택 |

> API 키가 없으면 시뮬레이션 데이터로 자동 대체됩니다.

---

## 📡 API 레퍼런스

### `GET /api/v1/quick-optimize`

대시보드용 빠른 최적화 엔드포인트.

```
GET /api/v1/quick-optimize?gpu_count=4&job_hours=8&region=ap-northeast-2&workload=batch
```

### `POST /api/v1/optimize`

전체 옵션 최적화.

```json
{
  "current_region": "ap-northeast-2",
  "gpu_spec": { "model": "H100", "count": 4 },
  "job_duration_hours": 8,
  "require_data_compliance": false,
  "workload_type": "batch",
  "stateless": false
}
```

전체 API 문서: https://gridshifter-production.up.railway.app/docs

---

## 🧮 SSS 지수란?

**Stability-Saving Score** — 절감률과 안정성을 동시에 최적화하는 GridShifter 고유 지수입니다.

```
SSS = w1 × 절감률 − w2 × 재고부족리스크 − w3 × 스팟회수확률
    + 컴플라이언스 위반 패널티 (−100)
    + 지연 초과 패널티 (−100, LIVE 모드)
    + Cross-CSP 패널티 (−100, Stateful)
```

기본 가중치: `w1=0.50, w2=0.30, w3=0.20`

---

## 📦 기술 스택

| 레이어 | 기술 |
|---|---|
| 백엔드 | Python 3.11 · FastAPI · Uvicorn |
| 데이터 수집 | boto3 (AWS) · requests (Lambda/RunPod/ENTSO-E) |
| 최적화 엔진 | NumPy 배치 연산 |
| 프론트엔드 | Vanilla JS · HTML/CSS (프레임워크 없음) |
| 배포 | Railway (백엔드) · GitHub Pages (프론트엔드) |
| 결제 | Stripe (베타 기간 비활성화) |

---

## 🗺️ 로드맵

- [x] MVP: SSS 솔버 + 대시보드 + REST API
- [x] 실시간 API 연동 (AWS · Lambda Labs · RunPod · ENTSO-E)
- [x] CLI (`gridshifter.py`) — 원클릭 배포
- [x] Railway 프로덕션 배포
- [ ] Phase 2b: SSH 없이 Docker 자동 실행
- [ ] 다중 GPU 공급자 동시 비교 (GCP 추가)
- [ ] Stripe Pro 플랜 활성화
- [ ] Slack / Discord 알림 연동

---

## ⚠️ 주의사항

- CLI로 인스턴스를 생성하면 **실제 과금**이 발생합니다. `--dry-run`으로 먼저 확인하세요.
- 스팟 인스턴스는 공급자 사정에 따라 강제 회수될 수 있습니다.
- 베타 기간 동안 무료로 제공되며, 추후 Pro 플랜($19/월)이 도입될 예정입니다.

---

## 📄 라이선스

MIT License — 자유롭게 사용, 수정, 배포 가능합니다.
