"""
app.py — GridShifter AI
FastAPI 서버: 전력망 + 클라우드 GPU 최적 라우팅 API

CORS 정책
  개발/대시보드: 로컬 file://(Origin: null) + localhost 전 포트 허용
  운영:         환경변수 CORS_ORIGINS (쉼표 구분) 로 화이트리스트 교체
                예) CORS_ORIGINS=https://gridshifter.io,https://admin.gridshifter.io
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from dotenv import load_dotenv

# .env.txt → 환경변수 주입 (파일 없으면 조용히 무시)
_ENV_FILE = Path(__file__).parent / ".env.txt"
if _ENV_FILE.exists():
    load_dotenv(dotenv_path=_ENV_FILE, override=False)  # 이미 설정된 시스템 환경변수 우선
    logging.getLogger("gridshifter.app").info(".env.txt 로드 완료 (키 값은 로깅 안 함)")

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from engine import (
    ComputeRequest, GPUSpec, OptimizationResult, Solver,
    WorkloadType, WorkloadAlert, LatencyWarning, detect_workload_type,
    LIVE_ALERT_THRESHOLD_MS, _get_latency, _latency_tier,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gridshifter.app")


# ---------------------------------------------------------------------------
# CORS 화이트리스트
# ---------------------------------------------------------------------------

def _build_cors_origins() -> list[str]:
    """
    환경변수 CORS_ORIGINS 가 설정되면 그 목록을 사용.
    미설정이면 로컬 대시보드용 기본 허용 목록 반환.

    'null' 항목은 로컬 file:// 로 열린 index.html 의 Origin 처리용.
    """
    raw = os.environ.get("CORS_ORIGINS", "").strip()
    if raw:
        origins = [o.strip() for o in raw.split(",") if o.strip()]
        logger.info("CORS whitelist (env): %s", origins)
        return origins

    defaults = [
        "null",                        # file:// 로컬 대시보드
        "http://localhost",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8080",
        "http://127.0.0.1",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8080",
        "https://gridshifter-production.up.railway.app",
        "https://gridshifter0001.github.io",
        "https://gridshifter.app",
        "https://www.gridshifter.app",
    ]
    logger.info("CORS whitelist (default dev): %s", defaults)
    return defaults


_CORS_ORIGINS = _build_cors_origins()


# ---------------------------------------------------------------------------
# 앱 생명주기
# ---------------------------------------------------------------------------

_solver: Solver | None = None
_startup_time: float = 0.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _solver, _startup_time
    logger.info("GridShifter AI starting up…")
    _solver = Solver()
    _startup_time = time.time()
    yield
    logger.info("GridShifter AI shutting down.")


# ---------------------------------------------------------------------------
# FastAPI 앱
# ---------------------------------------------------------------------------

app = FastAPI(
    title="GridShifter AI",
    description=(
        "글로벌 전력망 가격과 클라우드 GPU 자원을 실시간 최적 매칭하여 "
        "비용 절감 및 안정성을 극대화하는 API"
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_origin_regex=r"http://localhost:\d+",   # 모든 localhost 포트 커버
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
    expose_headers=["X-Response-Time-Ms"],
    max_age=600,
)


# ---------------------------------------------------------------------------
# 미들웨어: 소요시간 로깅 + 요청 크기 제한
# ---------------------------------------------------------------------------

_MAX_BODY_BYTES = 64 * 1024  # 64 KB — 악성 대형 바디 차단

# ── 동시 요청 제한 (optimize는 CPU 집약적) ────────────────────────────────
_OPTIMIZE_CONCURRENCY = int(os.environ.get("OPTIMIZE_CONCURRENCY", "10"))
_optimize_semaphore = asyncio.Semaphore(_OPTIMIZE_CONCURRENCY)

# ── 간이 IP 기반 레이트 리미터 ────────────────────────────────────────────
import collections
_RATE_LIMIT_RPM = int(os.environ.get("RATE_LIMIT_RPM", "30"))  # IP당 분당 최대 요청
_rate_counters: dict[str, collections.deque] = {}
_rate_lock = asyncio.Lock()


async def _check_rate_limit(ip: str) -> bool:
    """True = 허용, False = 초과."""
    if _RATE_LIMIT_RPM <= 0:
        return True
    now = time.time()
    async with _rate_lock:
        dq = _rate_counters.setdefault(ip, collections.deque())
        # 1분 이전 항목 제거
        while dq and now - dq[0] > 60:
            dq.popleft()
        if len(dq) >= _RATE_LIMIT_RPM:
            return False
        dq.append(now)
        return True


@app.middleware("http")
async def guard_and_log(request: Request, call_next):
    # 요청 바디 크기 사전 검사
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > _MAX_BODY_BYTES:
        return JSONResponse(
            status_code=413,
            content={"error": "Request body too large.", "max_bytes": _MAX_BODY_BYTES},
        )

    # 레이트 리미트 체크 (헬스체크 제외)
    if request.url.path != "/api/v1/health":
        client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown").split(",")[0].strip()
        if not await _check_rate_limit(client_ip):
            return JSONResponse(
                status_code=429,
                content={"error": "Too many requests. Please slow down.", "retry_after_seconds": 60},
                headers={"Retry-After": "60"},
            )

    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "%s %s → %d (%.1f ms) origin=%s",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
        request.headers.get("origin", "-"),
    )
    response.headers["X-Response-Time-Ms"] = str(elapsed_ms)
    return response


# ---------------------------------------------------------------------------
# 공통 에러 응답 포맷
# ---------------------------------------------------------------------------

def _err(status: int, message: str, detail: dict | None = None) -> JSONResponse:
    body: dict = {"error": message, "status": status}
    if detail:
        body["detail"] = detail
    return JSONResponse(status_code=status, content=body)


# ---------------------------------------------------------------------------
# 헬스 체크
# ---------------------------------------------------------------------------

@app.get("/api/v1/health", summary="서비스 헬스 체크", tags=["System"])
async def health_check():
    uptime = round(time.time() - _startup_time, 1) if _startup_time else 0
    return {
        "status": "ok",
        "service": "GridShifter AI",
        "version": "0.1.0",
        "uptime_seconds": uptime,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# POST /api/v1/optimize
# ---------------------------------------------------------------------------

class OptimizeRequest(ComputeRequest):
    """POST /api/v1/optimize 요청 바디."""
    pass


@app.post(
    "/api/v1/optimize",
    response_model=OptimizationResult,
    summary="GPU 클라우드 최적 라우팅 계산",
    description=(
        "컴퓨팅 요청 스펙을 받아 전 세계 클라우드 리전의 전력+GPU 비용을 "
        "SSS 지수로 평가하고 최적 리전을 추천합니다."
    ),
    tags=["Optimization"],
    responses={
        200: {"description": "최적 라우팅 결과"},
        400: {"description": "요청 파라미터 오류"},
        413: {"description": "요청 바디 크기 초과"},
        422: {"description": "Pydantic 유효성 오류"},
        503: {"description": "서버 초기화 중"},
    },
)
async def optimize(payload: OptimizeRequest):
    if _solver is None:
        raise HTTPException(status_code=503, detail="Solver not initialized.")

    if _optimize_semaphore.locked() and _optimize_semaphore._value == 0:
        raise HTTPException(status_code=503, detail="Server busy. Please retry in a moment.")

    async with _optimize_semaphore:
        try:
            loop = asyncio.get_event_loop()
            result: OptimizationResult = await loop.run_in_executor(
                None, partial(_solver.find_optimized_routing, payload)
            )
            return result
        except ValueError as exc:
            logger.warning("Validation error: %s", exc)
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            logger.exception("Optimize error: %s", exc)
            raise HTTPException(status_code=500, detail="Internal optimization error.")


# ---------------------------------------------------------------------------
# GET /api/v1/quick-optimize  (프론트엔드 대시보드용)
# ---------------------------------------------------------------------------

@app.get(
    "/api/v1/quick-optimize",
    response_model=OptimizationResult,
    summary="빠른 최적 라우팅 (대시보드용)",
    description=(
        "GPU 수량과 작업 시간만으로 서울 리전 기준 최적 라우팅을 즉시 반환합니다. "
        "index.html 대시보드가 직접 호출하는 엔드포인트입니다."
    ),
    tags=["Optimization"],
)
async def quick_optimize(
    gpu_count: int = Query(default=4, ge=1, le=512, description="H100 GPU 수량 (1–512)"),
    job_hours: float = Query(default=8.0, ge=0.5, le=8760.0, description="작업 시간 (시간 단위, 0.5–8760)"),
    require_compliance: bool = Query(default=False, description="데이터 컴플라이언스 강제 여부"),
    region: str = Query(default="ap-northeast-2", description="베이스라인 리전 ID"),
    workload: str = Query(default="batch", description="워크로드 타입: batch | live | auto"),
    inbound_ports: str = Query(default="", description="인바운드 포트 (쉼표 구분, 예: '80,443,8080')"),
    docker_command: str = Query(default="", description="도커 실행 명령어 (자동 감지용)"),
    latency_risk_ack: bool = Query(default=False, description="지연 리스크 인지 후 강행 여부"),
    stateless: bool = Query(default=False, description="Stateless 워크로드 여부 (Cross-CSP 허용)"),
    dataset_gb: float = Query(default=0.0, ge=0.0, le=1000000.0, description="데이터셋 크기 GB (0=이그레스 무시)"),
    data_source_region: str = Query(default="", description="데이터 저장 리전 (비어있으면 current_region)"),
):
    if _solver is None:
        raise HTTPException(status_code=503, detail="Solver not initialized.")

    import re
    if not re.fullmatch(r"[a-z0-9\-]{3,32}", region):
        raise HTTPException(status_code=400, detail=f"Invalid region format: '{region}'.")

    # 포트 파싱
    parsed_ports: list[int] = []
    if inbound_ports.strip():
        try:
            parsed_ports = [int(p.strip()) for p in inbound_ports.split(",") if p.strip()]
        except ValueError:
            raise HTTPException(status_code=400, detail="inbound_ports must be comma-separated integers.")

    try:
        wl_type = WorkloadType(workload)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"workload must be batch | live | auto (got '{workload}').")

    req = ComputeRequest(
        current_region=region,
        gpu_spec=GPUSpec(model="H100", count=gpu_count),
        job_duration_hours=job_hours,
        require_data_compliance=require_compliance,
        workload_type=wl_type,
        inbound_ports=parsed_ports,
        docker_command=docker_command,
        latency_risk_acknowledged=latency_risk_ack,
        stateless=stateless,
        dataset_size_gb=dataset_gb,
        data_source_region=data_source_region,
    )
    async with _optimize_semaphore:
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, partial(_solver.find_optimized_routing, req)
            )
            # 익명 피드백 자동 수집
            if result.recommended:
                rec = result.recommended
                client_ip = request.headers.get("x-forwarded-for", "0.0.0.0").split(",")[0].strip()
                ua = request.headers.get("user-agent", "")
                record_feedback(FeedbackEvent(
                    event_type="recommendation",
                    session_id=_session_id(client_ip, ua),
                    recommended_region=rec.region,
                    estimated_cost_usd=rec.total_cost_with_egress_usd or rec.estimated_total_cost_usd,
                    gpu_count=req.gpu_spec.count,
                    job_hours=req.job_duration_hours,
                    workload_type=req.workload_type.value,
                    dataset_gb=req.dataset_size_gb,
                    egress_cost_usd=rec.egress_cost_usd,
                    provider=rec.provider,
                    savings_pct=rec.savings_pct,
                    sss_score=rec.sss_score,
                ))
            return result
        except Exception as exc:
            logger.exception("quick-optimize error: %s", exc)
            raise HTTPException(status_code=500, detail="Internal optimization error.")


# ---------------------------------------------------------------------------
# POST /api/v1/check-workload  — Pre-Billing Alert 전용 경량 체크
# ---------------------------------------------------------------------------

class WorkloadCheckRequest(BaseModel):
    inbound_ports: list[int] = []
    docker_command: str = ""
    target_regions: list[str] = []

    model_config = {"str_max_length": 512}

    def model_post_init(self, __context):
        if len(self.target_regions) > 60:
            raise ValueError("target_regions must have at most 60 items.")
        if len(self.inbound_ports) > 20:
            raise ValueError("inbound_ports must have at most 20 items.")
        if len(self.docker_command) > 512:
            raise ValueError("docker_command too long (max 512 chars).")


class WorkloadCheckResponse(BaseModel):
    detected_workload: WorkloadType
    alert: WorkloadAlert
    latency_safe_regions: list[str]


@app.post(
    "/api/v1/check-workload",
    response_model=WorkloadCheckResponse,
    summary="워크로드 타입 자동 감지 + Pre-Billing Alert 사전 체크",
    description=(
        "배포 전 포트/명령어를 분석해 BATCH/LIVE 여부를 판단하고, "
        "LIVE 감지 시 지연 리스크 경고(3-choice)를 반환합니다."
    ),
    tags=["Workload"],
)
async def check_workload(body: WorkloadCheckRequest):
    detected = detect_workload_type(body.inbound_ports, body.docker_command)

    # 고지연 경고 구성
    high_latency: list[LatencyWarning] = []
    safe: list[str] = []

    for r in body.target_regions:
        ms = _get_latency(r)
        if ms > LIVE_ALERT_THRESHOLD_MS:
            high_latency.append(LatencyWarning(
                region=r, latency_ms=ms,
                latency_tier=_latency_tier(ms).value,
                provider="unknown",
            ))
        else:
            safe.append(r)

    alert_triggered = detected == WorkloadType.LIVE and bool(high_latency)
    alert = WorkloadAlert(
        triggered=alert_triggered,
        detected_workload=detected,
        alert_type="latency_risk" if alert_triggered else "none",
        high_latency_regions=high_latency,
        threshold_ms=LIVE_ALERT_THRESHOLD_MS,
        message=(
            f"실시간 서비스 감지: {len(high_latency)}개 리전이 {LIVE_ALERT_THRESHOLD_MS:.0f}ms 초과입니다."
            if alert_triggered else ""
        ),
        user_choices=[
            {"id": "lock_local",   "label": "[권장] 아시아 리전 고정",    "action": "set latency_threshold_ms=80"},
            {"id": "accept_risk",  "label": "위험 감수 강행",              "action": "set latency_risk_acknowledged=true"},
            {"id": "switch_batch", "label": "배치 작업으로 변경",          "action": "set workload_type=batch"},
        ] if alert_triggered else [],
    )

    return WorkloadCheckResponse(
        detected_workload=detected,
        alert=alert,
        latency_safe_regions=safe,
    )


# ---------------------------------------------------------------------------
# 전역 예외 핸들러 (Pydantic ValidationError 포함)
# ---------------------------------------------------------------------------

@app.exception_handler(ValidationError)
async def pydantic_exception_handler(request: Request, exc: ValidationError):
    logger.warning("Pydantic validation error on %s: %s", request.url.path, exc)
    return _err(
        422,
        "Request validation failed.",
        {"fields": exc.errors(include_url=False)},
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s: %s", request.url.path, exc)
    return _err(500, "An unexpected error occurred. Please try again later.")


# ---------------------------------------------------------------------------
# 결제 / 인증 라우터
# ---------------------------------------------------------------------------
from stripe_billing import (
    BILLING_ENABLED, PLANS,
    can_deploy, generate_api_key, get_plan_info,
    create_checkout_session, handle_webhook,
)
from feedback import FeedbackEvent, _session_id, record as record_feedback, get_stats


class RegisterRequest(BaseModel):
    email: str


class CheckoutRequest(BaseModel):
    api_key: str
    success_url: str = ""
    cancel_url: str = ""


@app.post("/api/v1/billing/register", tags=["billing"])
async def register(body: RegisterRequest):
    """이메일로 API 키 발급 (Free 플랜 시작)."""
    if not body.email or "@" not in body.email:
        raise HTTPException(400, "올바른 이메일을 입력해 주세요.")
    key = generate_api_key(body.email)
    plan = get_plan_info(key)
    return {
        "api_key": key,
        "plan":    plan,
        "message": "API 키 발급 완료. X-API-Key 헤더에 포함하거나 gridshifter.py --api-key 옵션으로 사용하세요.",
    }


@app.get("/api/v1/billing/plan", tags=["billing"])
async def get_plan(x_api_key: str = Query(default="")):
    """현재 플랜 및 배포 잔여 횟수 조회."""
    return get_plan_info(x_api_key or None)


@app.get("/api/v1/billing/plans", tags=["billing"])
async def list_plans():
    """전체 플랜 목록 + 과금 활성화 여부 반환 (프론트 프라이싱 페이지용)."""
    return {
        "billing_enabled": BILLING_ENABLED,
        "plans": PLANS,
        "beta_message": "베타 기간 동안 Pro 기능이 무료로 제공됩니다." if not BILLING_ENABLED else None,
    }


@app.post("/api/v1/billing/checkout", tags=["billing"])
async def create_checkout(body: CheckoutRequest):
    """Stripe Checkout 세션 생성 → 결제 URL 반환."""
    if not BILLING_ENABLED:
        return {"billing_enabled": False, "message": "베타 기간 무료 오픈 중"}

    rec = get_plan_info(body.api_key)
    if rec.get("plan") == "pro":
        return {"message": "이미 Pro 플랜입니다."}

    base = os.environ.get("PUBLIC_URL", "http://localhost:8000")
    success = body.success_url or f"{base}/dashboard?upgrade=success"
    cancel  = body.cancel_url  or f"{base}/dashboard?upgrade=cancel"

    try:
        url = create_checkout_session(
            email=rec.get("email", ""),
            api_key=body.api_key,
            success_url=success,
            cancel_url=cancel,
        )
        return {"checkout_url": url}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.get("/api/v1/stats", tags=["Analytics"])
async def stats():
    """수집된 익명 피드백 통계 (채택률, 절감률, 인기 리전 등)."""
    return get_stats()


class AdoptionFeedback(BaseModel):
    session_id: str
    recommended_region: str
    adopted: bool
    actual_cost_usd: float = 0.0


@app.post("/api/v1/feedback/adoption", tags=["Analytics"])
async def adoption_feedback(body: AdoptionFeedback):
    """추천 채택 여부 피드백 수신 (대시보드에서 자동 호출)."""
    record_feedback(FeedbackEvent(
        event_type="adoption",
        session_id=body.session_id,
        recommended_region=body.recommended_region,
        adopted=body.adopted,
        actual_cost_usd=body.actual_cost_usd,
    ))
    return {"status": "recorded"}


@app.post("/api/v1/billing/webhook", tags=["billing"], include_in_schema=False)
async def stripe_webhook(request: Request):
    """Stripe Webhook 수신 처리 (결제 완료 → Pro 업그레이드)."""
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    try:
        result = handle_webhook(payload, sig_header)
        return result
    except ValueError as exc:
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
    )
