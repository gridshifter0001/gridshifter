"""
engine.py — GridShifter AI
최적 라우팅 Solver: SSS 지수 + 워크로드 분기 + 지연 가드레일

워크로드 분류
  BATCH : AI 학습/렌더링 등 종료 시점이 정해진 작업
          → 지연 제약 없음, 글로벌 대안 클라우드 허용, 70%+ 절감 타깃
  LIVE  : LLM 추론 서버, 웹서비스 등 24시간 유저 통신 작업
          → 지연 임계값(기본 80ms) 초과 리전 배제, Tier-1 거인 클라우드 위주 매칭
  AUTO  : inbound_ports 비어있으면 BATCH, 있으면 LIVE로 자동 판단

Pre-Billing Alert
  LIVE 워크로드에서 추천 리전의 지연이 임계값을 초과하면 경고를 트리거하고
  latency_risk_acknowledged=False 이면 추천을 보류합니다.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import numpy as np
from pydantic import BaseModel, Field, field_validator, model_validator

from scrapers import CloudInventory, CloudTelemetryScraper, PowerGridScraper, PowerPrice

logger = logging.getLogger("gridshifter.engine")

# ---------------------------------------------------------------------------
# SSS 가중치 기본값
# ---------------------------------------------------------------------------
DEFAULT_W1 = 0.50
DEFAULT_W2 = 0.30
DEFAULT_W3 = 0.20

COMPLIANCE_PENALTY  = -100.0   # 보안 규제 위반 감점
LATENCY_PENALTY     = -100.0   # LIVE 지연 초과 감점 (compliance와 동급)
CROSS_CSP_PENALTY   = -100.0   # LIVE + stateless=False 에서 Cross-CSP 스왑 차단 감점

# ---------------------------------------------------------------------------
# 워크로드 타입 & 지연 프로파일
# ---------------------------------------------------------------------------

class WorkloadType(str, Enum):
    BATCH = "batch"   # 배치: 지연 제약 없음
    LIVE  = "live"    # 실시간 서비스: 지연 가드레일 적용
    AUTO  = "auto"    # 자동 감지 (inbound_ports 유무로 판단)


class LatencyTier(str, Enum):
    ULTRA_LOW  = "ultra_low"   # <30ms  동일/인접 리전
    LOW        = "low"         # 30–80ms 동아시아
    MEDIUM     = "medium"      # 80–150ms 동남아/미서부
    HIGH       = "high"        # >150ms 미동부/유럽 → LIVE 불가


# 서울 기준 왕복 지연(RTT) 근삿값 (ms)
# 실제 운영 시 CloudPing / Catchpoint 측정값으로 교체 권장
_LATENCY_FROM_SEOUL_MS: dict[str, float] = {
    # AWS
    "ap-northeast-2": 5.0,     # 서울 (자기 자신)
    "ap-northeast-1": 32.0,    # 도쿄
    "ap-northeast-3": 35.0,    # 오사카
    "ap-east-1":      38.0,    # 홍콩
    "ap-southeast-1": 75.0,    # 싱가포르
    "ap-southeast-2": 135.0,   # 시드니
    "ap-south-1":     110.0,   # 뭄바이
    "us-east-1":      185.0,   # 버지니아
    "us-east-2":      185.0,   # 오하이오
    "us-west-1":      155.0,   # 캘리포니아
    "us-west-2":      160.0,   # 오레곤
    "eu-west-1":      240.0,   # 아일랜드
    "eu-west-2":      242.0,   # 런던
    "eu-central-1":   245.0,   # 프랑크푸르트
    "eu-north-1":     250.0,   # 스톡홀름
    # Lambda Labs
    "us-tx-3":        210.0,   # 텍사스
    "us-az-1":        195.0,   # 애리조나
    "us-west-1":      155.0,   # 캘리포니아
    "eu-central-1":   245.0,   # 유럽
    "me-1":           120.0,   # 중동
    "asia-1":         40.0,    # 아시아 (Lambda)
    # ENTSO-E / EU cloud regions
    "eu-west-3":      250.0,   # 프랑스 (Paris)
    "eu-west-2":      242.0,   # 영국 (London)
    "eu-north-1":     255.0,   # 스웨덴 (Stockholm)
    # RunPod (데이터센터 기반 근삿값)
    "runpod-us-h100": 195.0,   # RunPod US H100
    "runpod-us-a100": 195.0,   # RunPod US A100
    "runpod-us-h200": 195.0,   # RunPod US H200
    "runpod-us-b200": 195.0,   # RunPod US B200
    "runpod-us-l40":  195.0,   # RunPod US L40
    "runpod-eu-h100": 248.0,   # RunPod EU H100
    "runpod-eu-a100": 248.0,   # RunPod EU A100
    "runpod-eu-h200": 248.0,   # RunPod EU H200
    "runpod-eu-b200": 248.0,   # RunPod EU B200
    "runpod-eu-l40":  248.0,   # RunPod EU L40
}

_DEFAULT_LATENCY_MS = 200.0  # 알 수 없는 리전 기본값

# LIVE 워크로드: 지연 임계값 (ms)
LIVE_LATENCY_THRESHOLD_MS = 80.0   # 동아시아 권역 상한
LIVE_ALERT_THRESHOLD_MS   = 150.0  # 이 이상이면 Pre-Billing Alert 트리거


def _get_latency(region: str) -> float:
    return _LATENCY_FROM_SEOUL_MS.get(region, _DEFAULT_LATENCY_MS)


def _latency_tier(ms: float) -> LatencyTier:
    if ms < 30:   return LatencyTier.ULTRA_LOW
    if ms < 80:   return LatencyTier.LOW
    if ms < 150:  return LatencyTier.MEDIUM
    return LatencyTier.HIGH


# ---------------------------------------------------------------------------
# 워크로드 자동 감지 — 포트 + 명령어 키워드 복합 신호
# ---------------------------------------------------------------------------

_LIVE_COMMAND_KEYWORDS = {
    "serve", "server", "api", "uvicorn", "gunicorn", "nginx",
    "flask", "fastapi", "django", "node", "streamlit", "gradio",
    "inference", "triton", "torchserve", "bentoml", "ray serve",
}


def detect_workload_type(
    inbound_ports: list[int],
    docker_command: str = "",
) -> WorkloadType:
    """
    포트 개방 여부 + 명령어 키워드로 워크로드 타입을 자동 감지합니다.

    포트 단독 신호는 오탐(TensorBoard, Jupyter 등)이 많으므로
    명령어 키워드를 1순위로 사용하고 포트는 2순위 보조 신호로 사용합니다.
    """
    cmd_lower = docker_command.lower()

    # 1순위: 서버 프로세스 키워드가 명령어에 있으면 LIVE
    if any(kw in cmd_lower for kw in _LIVE_COMMAND_KEYWORDS):
        return WorkloadType.LIVE

    # 2순위: 인바운드 포트가 있고 모니터링 포트(8888/6006)만이 아니면 LIVE
    monitoring_only_ports = {8888, 6006, 8097}  # Jupyter, TensorBoard, Visdom
    non_monitoring = [p for p in inbound_ports if p not in monitoring_only_ports]
    if non_monitoring:
        return WorkloadType.LIVE

    return WorkloadType.BATCH


# ---------------------------------------------------------------------------
# Pre-Billing Alert 모델
# ---------------------------------------------------------------------------

class LatencyWarning(BaseModel):
    region: str
    latency_ms: float
    latency_tier: str
    provider: str


class WorkloadAlert(BaseModel):
    triggered: bool = False
    detected_workload: WorkloadType = WorkloadType.BATCH
    alert_type: str = "none"               # "latency_risk" | "cross_csp_stateful" | "latency_risk_and_cross_csp" | "none"
    high_latency_regions: list[LatencyWarning] = []
    cross_csp_blocked_regions: list[str] = []  # LIVE + stateless=False 시 차단된 Cross-CSP 리전 목록
    threshold_ms: float = LIVE_ALERT_THRESHOLD_MS
    message: str = ""
    user_choices: list[dict] = []


# ---------------------------------------------------------------------------
# Pydantic V2 요청 스키마
# ---------------------------------------------------------------------------

class GPUSpec(BaseModel):
    model: str = Field(default="H100", description="GPU 모델명 (예: H100, A100, B200)")
    count: int = Field(ge=1, le=1024, description="요청 GPU 수량")


class ComputeRequest(BaseModel):
    """유저의 컴퓨팅 요청 스펙"""

    current_region: str = Field(
        description="현재 사용 중인 리전 ID",
        examples=["ap-northeast-2"],
    )
    gpu_spec: GPUSpec = Field(description="GPU 사양 및 수량")
    job_duration_hours: float = Field(
        ge=0.5, le=8760, description="예상 작업 시간 (시간 단위)"
    )

    # ── 워크로드 분류 ──────────────────────────────────────────────────────
    workload_type: WorkloadType = Field(
        default=WorkloadType.AUTO,
        description=(
            "batch: 학습/렌더링 등 배치 작업 (글로벌 대안 클라우드 허용) | "
            "live: 추론 API/웹서버 (지연 가드레일 적용) | "
            "auto: inbound_ports/docker_command 으로 자동 판단"
        ),
    )
    inbound_ports: list[int] = Field(
        default_factory=list,
        description="외부에 개방할 TCP 포트 목록 (예: [80, 443, 8080]). auto 감지에 사용.",
    )
    docker_command: str = Field(
        default="",
        description="실행할 도커 명령어 (예: 'uvicorn main:app'). auto 감지에 사용.",
    )
    latency_threshold_ms: float = Field(
        default=LIVE_LATENCY_THRESHOLD_MS,
        ge=5.0, le=500.0,
        description="LIVE 모드에서 허용할 최대 지연 (ms). 초과 리전은 추천 제외.",
    )
    latency_risk_acknowledged: bool = Field(
        default=False,
        description=(
            "Pre-Billing Alert 발동 시 유저가 지연 리스크를 인지하고 강행을 선택했는지 여부. "
            "True이면 임계값 초과 리전도 추천 허용."
        ),
    )
    stateless: bool = Field(
        default=False,
        description=(
            "워크로드가 외부 스토리지(DB/Redis 등)에 상태를 저장하는 stateless 아키텍처인지 여부. "
            "True이면 LIVE 모드에서 Cross-CSP 스왑 허용. "
            "False이면 LIVE 모드에서 Cross-CSP 후보 리전을 차단하고 WorkloadAlert에 표시."
        ),
    )

    # ── 컴플라이언스 ──────────────────────────────────────────────────────
    require_data_compliance: bool = Field(
        default=False,
        description="데이터 보안/컴플라이언스 준수 필수 여부",
    )
    compliance_allowed_regions: list[str] = Field(
        default_factory=list,
        description="컴플라이언스 허용 리전 목록",
    )

    # ── SSS 가중치 ────────────────────────────────────────────────────────
    w1: float = Field(default=DEFAULT_W1, ge=0.0, le=1.0, description="절감률 가중치")
    w2: float = Field(default=DEFAULT_W2, ge=0.0, le=1.0, description="재고 부족 리스크 가중치")
    w3: float = Field(default=DEFAULT_W3, ge=0.0, le=1.0, description="스팟 회수 확률 가중치")

    @field_validator("current_region")
    @classmethod
    def strip_region(cls, v: str) -> str:
        return v.strip().lower()

    @model_validator(mode="after")
    def resolve_workload_and_weights(self) -> "ComputeRequest":
        # AUTO → 실제 타입으로 해소
        if self.workload_type == WorkloadType.AUTO:
            self.workload_type = detect_workload_type(
                self.inbound_ports, self.docker_command
            )

        # 가중치 합계 검증
        total = round(self.w1 + self.w2 + self.w3, 6)
        if not (0.99 <= total <= 1.01):
            raise ValueError(f"w1+w2+w3 must sum to 1.0 (got {total})")
        return self


# ---------------------------------------------------------------------------
# 결과 스키마
# ---------------------------------------------------------------------------

class RegionScore(BaseModel):
    region: str
    provider: str
    gpu_model: str
    available_units: int
    on_demand_price_usd_per_hour: float
    spot_price_usd_per_hour: float
    power_price_usd_per_kwh: float
    estimated_total_cost_usd: float
    baseline_cost_usd: float
    savings_pct: float
    inventory_shortage_risk: float
    spot_revocation_probability: float
    compliance_violation: bool
    latency_ms: float = 0.0
    latency_tier: str = LatencyTier.ULTRA_LOW
    latency_excluded: bool = False        # LIVE 모드에서 지연 초과로 배제됨
    cross_csp_excluded: bool = False      # LIVE + stateless=False 에서 Cross-CSP로 배제됨
    availability_zone: Optional[str] = None  # AZ 레벨 분리 시 세부 AZ (예: us-east-1a)
    sss_score: float
    is_recommended: bool = False


class OptimizationResult(BaseModel):
    request_summary: ComputeRequest
    resolved_workload: WorkloadType      # AUTO 해소 후 실제 타입
    baseline_region: str
    baseline_cost_usd: float
    recommended: Optional[RegionScore]
    all_candidates: list[RegionScore]
    workload_alert: WorkloadAlert        # Pre-Billing Alert (triggered=False이면 정상)
    timestamp: str


# ---------------------------------------------------------------------------
# Solver
# ---------------------------------------------------------------------------

_POWER_REGION_MAP: dict[str, str] = {
    # KEPCO / ERCOT: power region id → cloud region id
    "kr-seoul":  "ap-northeast-2",
    "us-texas":  "us-tx-3",
    # ENTSO-E: region id가 곧 cloud region id (항등 매핑 — 명시적으로 나열)
    "ie-dublin":    "ie-dublin",
    "eu-west-3":    "eu-west-3",
    "eu-central-1": "eu-central-1",
    "eu-west-1":    "eu-west-1",
    "eu-north-1":   "eu-north-1",
}

_GPU_POWER_KW = 0.70   # H100 SXM5 ≈ 700W (B200은 1000W이지만 동일 근사 사용)

_NON_COMPLIANT_REGIONS: set[str] = set()


class Solver:
    """
    SSS(Stability-Saving Score) 지수 기반 최적 라우팅 엔진.

    BATCH 모드: 글로벌 모든 리전 대상, 전력+GPU 비용 최소화
    LIVE 모드:  latency_threshold_ms 이하 리전만 추천 허용
               임계값 초과 시 Pre-Billing Alert → latency_risk_acknowledged 필요
    """

    def __init__(self) -> None:
        self._power_scraper = PowerGridScraper()
        self._cloud_scraper = CloudTelemetryScraper()

    # ------------------------------------------------------------------
    # 내부 유틸
    # ------------------------------------------------------------------

    def _get_power_prices(self) -> dict[str, PowerPrice]:
        raw = self._power_scraper.fetch_all()
        mapping: dict[str, PowerPrice] = {}
        for p in raw:
            # 명시적 매핑이 있으면 사용, 없으면 region 자체를 클라우드 리전 ID로 직접 사용
            cloud_region = _POWER_REGION_MAP.get(p.region, p.region)
            mapping[cloud_region] = p
        return mapping

    @staticmethod
    def _compute_total_cost(
        inv: CloudInventory,
        power_price: Optional[PowerPrice],
        gpu_count: int,
        duration_hours: float,
    ) -> float:
        compute_cost = inv.spot_price_usd_per_hour * gpu_count * duration_hours
        electricity = 0.0
        if power_price:
            electricity = max(
                0.0,
                _GPU_POWER_KW * gpu_count * duration_hours * power_price.price_usd_per_kwh,
            )
        return round(compute_cost + electricity, 4)

    @staticmethod
    def _inventory_shortage_risk(available: int, requested: int) -> float:
        if available >= requested:
            return 0.0
        return round(1.0 - (available / max(requested, 1)), 4)

    @staticmethod
    def _compute_sss_batch(
        savings_rates: np.ndarray,
        inventory_risks: np.ndarray,
        revocation_probs: np.ndarray,
        w1: float, w2: float, w3: float,
        compliance_violations: np.ndarray,
        latency_violations: np.ndarray,
        cross_csp_violations: np.ndarray,
    ) -> np.ndarray:
        """SSS 배치 행렬 연산. 컴플라이언스·지연·Cross-CSP 위반 모두 PENALTY 적용."""
        scores = (
            w1 * savings_rates
            - w2 * inventory_risks
            - w3 * revocation_probs
            + compliance_violations * COMPLIANCE_PENALTY
            + latency_violations    * LATENCY_PENALTY
            + cross_csp_violations  * CROSS_CSP_PENALTY
        )
        return np.round(scores, 6)

    def _is_compliance_violation(self, region: str, req: ComputeRequest) -> bool:
        if not req.require_data_compliance:
            return False
        if region in _NON_COMPLIANT_REGIONS:
            return True
        if req.compliance_allowed_regions:
            return region not in req.compliance_allowed_regions
        return region != req.current_region

    def _is_latency_excluded(self, latency_ms: float, req: ComputeRequest) -> bool:
        """LIVE 모드에서 지연 임계값을 초과하고 유저가 위험 수용을 하지 않았으면 배제."""
        if req.workload_type != WorkloadType.LIVE:
            return False
        if req.latency_risk_acknowledged:
            return False
        return latency_ms > req.latency_threshold_ms

    @staticmethod
    def _is_cross_csp_excluded(
        inv_provider: str, current_provider: str, req: ComputeRequest
    ) -> bool:
        """LIVE + stateless=False 에서 현재 CSP와 다른 프로바이더 리전을 배제."""
        if req.workload_type != WorkloadType.LIVE:
            return False
        if req.stateless:
            return False
        return inv_provider != current_provider

    def _build_workload_alert(
        self,
        req: ComputeRequest,
        inventories: list[CloudInventory],
        current_provider: str = "",
    ) -> WorkloadAlert:
        """Pre-Billing Alert 생성.

        LIVE 워크로드에서:
          - 고지연 리전이 존재하면 latency_risk 트리거
          - stateless=False 이고 Cross-CSP 후보가 있으면 cross_csp_blocked_regions 에 표시
        """
        if req.workload_type != WorkloadType.LIVE:
            return WorkloadAlert(triggered=False, detected_workload=req.workload_type)

        # ── 1. 고지연 리전 목록 (AZ 중복 제거: region 기준 dedup) ─────────
        seen_regions: set[str] = set()
        high_latency: list[LatencyWarning] = []
        for inv in inventories:
            if inv.region in seen_regions:
                continue
            seen_regions.add(inv.region)
            lat = _get_latency(inv.region)
            if lat > LIVE_ALERT_THRESHOLD_MS:
                high_latency.append(LatencyWarning(
                    region=inv.region,
                    latency_ms=lat,
                    latency_tier=_latency_tier(lat).value,
                    provider=inv.provider,
                ))

        # ── 2. Cross-CSP 차단 리전 목록 (LIVE + stateless=False) ──────────
        cross_csp_blocked: list[str] = []
        if not req.stateless and current_provider:
            seen_csp: set[str] = set()
            for inv in inventories:
                if inv.region in seen_csp:
                    continue
                seen_csp.add(inv.region)
                if inv.provider != current_provider:
                    cross_csp_blocked.append(f"{inv.region} ({inv.provider})")

        # ── 3. 트리거 여부 결정 (지연 기반, 기존 시맨틱 유지) ─────────────
        latency_triggered = bool(high_latency) and not req.latency_risk_acknowledged

        if not latency_triggered and not cross_csp_blocked:
            return WorkloadAlert(
                triggered=False,
                detected_workload=WorkloadType.LIVE,
                high_latency_regions=high_latency,
                cross_csp_blocked_regions=cross_csp_blocked,
            )

        # ── 4. 메시지 및 alert_type 구성 ──────────────────────────────────
        msg_parts: list[str] = []
        alert_type = "none"

        if latency_triggered:
            alert_type = "latency_risk"
            msg_parts.append(
                f"실시간 서비스(인바운드 포트 감지)가 요청되었습니다. "
                f"{len(high_latency)}개 해외 리전은 서울 기준 "
                f"{max(w.latency_ms for w in high_latency):.0f}ms 이상의 지연이 예상되어 "
                f"서비스 품질 저하가 우려됩니다."
            )

        if cross_csp_blocked:
            alert_type = (
                "latency_risk_and_cross_csp" if alert_type == "latency_risk"
                else "cross_csp_stateful"
            )
            msg_parts.append(
                f"Cross-CSP 스왑 불가(stateful): "
                f"{len(cross_csp_blocked)}개 리전({', '.join(cross_csp_blocked)})은 "
                f"현재 CSP({current_provider})와 다른 클라우드 프로바이더입니다. "
                f"stateless=true 설정 시에만 Cross-CSP 후보로 포함됩니다."
            )

        # ── 5. 유저 선택지 ────────────────────────────────────────────────
        user_choices: list[dict] = []
        if latency_triggered:
            user_choices += [
                {
                    "id": "lock_local",
                    "label": "[권장] 아시아 리전 고정",
                    "description": (
                        "비용 중심 필터를 끄고 80ms 이하 동아시아 리전(서울/도쿄/홍콩)만 대상으로 "
                        "안정성 중심 최적화를 수행합니다."
                    ),
                    "action": "set latency_threshold_ms=80, latency_risk_acknowledged=false",
                },
                {
                    "id": "accept_risk",
                    "label": "위험 감수 강행",
                    "description": (
                        "글로벌 서비스이거나 테스트 서버임을 확인했습니다. "
                        "latency_risk_acknowledged=true 로 재요청하면 해외 초저가 리전도 추천에 포함합니다."
                    ),
                    "action": "set latency_risk_acknowledged=true",
                },
                {
                    "id": "switch_batch",
                    "label": "배치 작업으로 변경",
                    "description": (
                        "포트 설정이 실수라면 workload_type=batch 로 변경해 "
                        "지연 제약 없이 최대 절감 리전을 탐색합니다."
                    ),
                    "action": "set workload_type=batch",
                },
            ]
        if cross_csp_blocked:
            user_choices.append({
                "id": "enable_stateless",
                "label": "Stateless 모드 활성화",
                "description": (
                    "워크로드가 상태를 외부 스토리지(DB/Redis 등)에 저장하는 stateless 아키텍처라면 "
                    "stateless=true 로 재요청하면 타 CSP 저렴한 리전도 후보에 포함됩니다."
                ),
                "action": "set stateless=true",
            })

        return WorkloadAlert(
            triggered=latency_triggered,
            detected_workload=WorkloadType.LIVE,
            alert_type=alert_type,
            high_latency_regions=high_latency,
            cross_csp_blocked_regions=cross_csp_blocked,
            threshold_ms=LIVE_ALERT_THRESHOLD_MS,
            message=" | ".join(msg_parts),
            user_choices=user_choices,
        )

    # ------------------------------------------------------------------
    # 공개 API
    # ------------------------------------------------------------------

    def find_optimized_routing(self, req: ComputeRequest) -> OptimizationResult:
        """
        전체 후보 리전에 대해 SSS 지수를 계산하고 최적 라우팅 결과를 반환합니다.

        LIVE 모드에서 고지연 리전이 감지되면 WorkloadAlert를 발동합니다.
        alert.triggered=True 이고 latency_risk_acknowledged=False이면
        recommended 는 None이 됩니다 (결제 전 경고 강제).

        LIVE + stateless=False 이면 Cross-CSP 후보에 CROSS_CSP_PENALTY 적용 및
        WorkloadAlert.cross_csp_blocked_regions 에 목록 표시.
        """
        power_map    = self._get_power_prices()
        inventories  = self._cloud_scraper.fetch_all()
        gpu_count    = req.gpu_spec.count
        duration     = req.job_duration_hours

        if not inventories:
            raise ValueError("No cloud inventory data available. All scrapers failed. Please retry later.")

        # 현재 리전의 CSP 결정 (Cross-CSP 가드레일 기준점)
        current_provider: str = next(
            (inv.provider for inv in inventories if inv.region == req.current_region),
            "AWS",
        )

        # Pre-Billing Alert 사전 생성 (Cross-CSP 정보 포함)
        alert = self._build_workload_alert(req, inventories, current_provider)

        # 베이스라인 비용
        baseline_inv  = next((i for i in inventories if i.region == req.current_region), None)
        baseline_cost = (
            self._compute_total_cost(
                baseline_inv, power_map.get(req.current_region), gpu_count, duration
            )
            if baseline_inv
            else 13.50 * gpu_count * duration
        )

        logger.info(
            "Baseline [%s] $%.2f | workload=%s | stateless=%s | csp=%s | alert=%s",
            req.current_region, baseline_cost,
            req.workload_type.value, req.stateless, current_provider, alert.triggered,
        )

        # ── 후보별 중간값 계산 ──────────────────────────────────────────
        (total_costs, savings_rates, inv_risks,
         revoc_probs, comp_viols, lat_viols,
         cross_csp_viols, latencies, power_prices_kwh) = ([] for _ in range(9))

        for inv in inventories:
            power_price    = power_map.get(inv.region)
            total_cost     = self._compute_total_cost(inv, power_price, gpu_count, duration)
            savings        = (baseline_cost - total_cost) / max(baseline_cost, 1e-9)
            risk           = self._inventory_shortage_risk(inv.available_units, gpu_count)
            comp_viol      = self._is_compliance_violation(inv.region, req)
            lat_ms         = _get_latency(inv.region)
            lat_excl       = self._is_latency_excluded(lat_ms, req)
            cross_csp_excl = self._is_cross_csp_excluded(inv.provider, current_provider, req)

            total_costs.append(total_cost)
            savings_rates.append(savings)
            inv_risks.append(risk)
            revoc_probs.append(inv.spot_revocation_probability)
            comp_viols.append(1.0 if comp_viol else 0.0)
            lat_viols.append(1.0 if lat_excl else 0.0)
            cross_csp_viols.append(1.0 if cross_csp_excl else 0.0)
            latencies.append(lat_ms)
            power_prices_kwh.append(power_price.price_usd_per_kwh if power_price else 0.0)

        # ── SSS 벡터 연산 ─────────────────────────────────────────────
        sss_scores = self._compute_sss_batch(
            savings_rates=np.array(savings_rates),
            inventory_risks=np.array(inv_risks),
            revocation_probs=np.array(revoc_probs),
            w1=req.w1, w2=req.w2, w3=req.w3,
            compliance_violations=np.array(comp_viols),
            latency_violations=np.array(lat_viols),
            cross_csp_violations=np.array(cross_csp_viols),
        )

        candidates = [
            RegionScore(
                region=inv.region,
                provider=inv.provider,
                gpu_model=inv.gpu_model,
                available_units=inv.available_units,
                on_demand_price_usd_per_hour=inv.on_demand_price_usd_per_hour,
                spot_price_usd_per_hour=inv.spot_price_usd_per_hour,
                power_price_usd_per_kwh=power_prices_kwh[i],
                estimated_total_cost_usd=total_costs[i],
                baseline_cost_usd=baseline_cost,
                savings_pct=round(savings_rates[i] * 100, 2),
                inventory_shortage_risk=inv_risks[i],
                spot_revocation_probability=inv.spot_revocation_probability,
                compliance_violation=bool(comp_viols[i]),
                latency_ms=latencies[i],
                latency_tier=_latency_tier(latencies[i]).value,
                latency_excluded=bool(lat_viols[i]),
                cross_csp_excluded=bool(cross_csp_viols[i]),
                availability_zone=inv.availability_zone,
                sss_score=float(sss_scores[i]),
            )
            for i, inv in enumerate(inventories)
        ]

        # SSS 내림차순 정렬 (저렴한 AZ가 자연스럽게 상위로 올라옴)
        sorted_idx = np.argsort(-sss_scores)
        candidates = [candidates[i] for i in sorted_idx]

        # 추천 선정
        recommended: Optional[RegionScore] = None

        def _is_eligible(c: RegionScore, ack: bool) -> bool:
            """추천 적격 여부: 현재 리전 제외 + 가드레일 통과."""
            if c.region == req.current_region:
                return False   # 이미 거기 있으므로 이동 추천 불필요
            if c.compliance_violation:
                return False
            if not ack and (c.latency_excluded or c.cross_csp_excluded):
                return False
            return True

        for c in candidates:
            if _is_eligible(c, req.latency_risk_acknowledged):
                c.is_recommended = True
                recommended = c
                break

        # 유효한 후보가 있으면 alert 비차단 처리
        if alert.triggered and recommended is not None:
            alert = alert.model_copy(update={"triggered": False})

        logger.info(
            "Optimization: %d candidates | recommended=%s (SSS=%.4f) | alert=%s",
            len(candidates),
            recommended.region if recommended else "BLOCKED_BY_ALERT",
            recommended.sss_score if recommended else float("nan"),
            alert.alert_type,
        )

        return OptimizationResult(
            request_summary=req,
            resolved_workload=req.workload_type,
            baseline_region=req.current_region,
            baseline_cost_usd=round(baseline_cost, 4),
            recommended=recommended,
            all_candidates=candidates,
            workload_alert=alert,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
