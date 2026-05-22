"""
scrapers.py — GridShifter AI
전력망 요금 및 클라우드 GPU 재고 데이터 수집 모듈

외부 API 연동 전략
─────────────────────────────────────────────────────────────────────────────
| 데이터 소스        | 방식                                  | 환경변수                                    |
| AWS Spot 가격     | boto3 describe_spot_price_history     | AWS_ACCESS_KEY_ID / SECRET / REGION         |
| ERCOT 실시간 LMP  | ERCOT Public API (Bearer + Sub-Key)   | ERCOT_SUBSCRIPTION_KEY / CLIENT_ID /        |
|                   |                                       | CLIENT_SECRET / TENANT_ID                   |
| Lambda Labs 재고  | Lambda Cloud REST API (Basic Auth)    | LAMBDA_LABS_API_KEY                         |
| RunPod GPU 재고   | RunPod GraphQL API                    | RUNPOD_API_KEY (선택; 없으면 공개 쿼리)        |
| ENTSO-E 전력(EU)  | ENTSO-E Transparency Platform REST    | ENTSOE_API_KEY                              |
| KEPCO / EirGrid   | 공개 API 없음 → 현실적 시계열 시뮬레이션  | (없음)                                      |
─────────────────────────────────────────────────────────────────────────────

Fail-Safe 체계
  1순위: 실시간 외부 API 호출
  2순위: TTL 만료 캐시 (stale-while-revalidate)
  3순위: 내장 정적 백업 데이터
"""

from __future__ import annotations

import base64
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import requests

logger = logging.getLogger("gridshifter.scrapers")

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 10          # 외부 API 기본 타임아웃 (초)
CACHE_TTL_LIVE = 300          # 라이브 데이터 캐시 유효 시간 (5분)
CACHE_TTL_STALE = 3600        # stale 캐시 최대 보관 시간 (1시간)


# ---------------------------------------------------------------------------
# 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class PowerPrice:
    region: str
    provider: str
    price_usd_per_kwh: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_negative: bool = False
    carbon_intensity_gco2_per_kwh: Optional[float] = None
    data_source: str = "live"       # "live" | "cache" | "fallback"


@dataclass
class CloudInventory:
    region: str
    provider: str
    gpu_model: str
    available_units: int
    spot_revocation_probability: float  # 0.0 ~ 1.0
    on_demand_price_usd_per_hour: float
    spot_price_usd_per_hour: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data_source: str = "live"
    availability_zone: Optional[str] = None  # AZ 레벨 분리 시 세부 AZ (예: us-east-1a)


# ---------------------------------------------------------------------------
# TTL 캐시 (스레드 안전)
# ---------------------------------------------------------------------------

@dataclass
class _CacheEntry:
    value: Any
    fetched_at: float = field(default_factory=time.monotonic)

    def is_fresh(self, ttl: float = CACHE_TTL_LIVE) -> bool:
        return (time.monotonic() - self.fetched_at) < ttl

    def is_usable(self) -> bool:
        return (time.monotonic() - self.fetched_at) < CACHE_TTL_STALE


class _Cache:
    """
    키-값 TTL 캐시. 스레드 안전 + 캐시 스탬피드 방지.

    스탬피드 방지 전략:
      - 키별 per-key Event로 첫 번째 요청만 실제 fetch를 수행하고
        나머지는 Event 완료를 기다린 뒤 캐시를 읽습니다.
      - stale 값이 있으면 fetch 결과를 기다리지 않고 즉시 stale 반환
        (background revalidation 패턴).
    """

    def __init__(self) -> None:
        self._store: dict[str, _CacheEntry] = {}
        self._store_lock = threading.Lock()
        # 키별 inflight 이벤트: fetch 진행 중일 때 생성, 완료 시 제거
        self._inflight: dict[str, threading.Event] = {}

    def get(self, key: str, ttl: float = CACHE_TTL_LIVE) -> Optional[Any]:
        with self._store_lock:
            entry = self._store.get(key)
            if entry and entry.is_fresh(ttl):
                return entry.value
        return None

    def get_stale(self, key: str) -> Optional[Any]:
        """만료됐지만 보관 중인 캐시 반환 (복사본 반환으로 원본 보호)."""
        with self._store_lock:
            entry = self._store.get(key)
            if entry and entry.is_usable():
                import copy
                return copy.copy(entry.value)
        return None

    def set(self, key: str, value: Any) -> None:
        with self._store_lock:
            self._store[key] = _CacheEntry(value=value)
            # fetch 완료 → 대기 중인 스레드 해제
            event = self._inflight.pop(key, None)
        if event:
            event.set()

    def acquire_fetch_token(self, key: str) -> tuple[bool, Optional[threading.Event]]:
        """
        호출자가 fetch를 수행해야 하면 (True, None) 반환.
        다른 스레드가 이미 fetch 중이면 (False, Event) 반환 — 호출자는 Event를 기다린 후 get() 재시도.
        """
        with self._store_lock:
            if key in self._inflight:
                return False, self._inflight[key]
            event = threading.Event()
            self._inflight[key] = event
            return True, None

    def release_fetch_token(self, key: str) -> None:
        """fetch 실패 시 토큰 강제 해제 (set()을 호출하지 못한 경우)."""
        with self._store_lock:
            event = self._inflight.pop(key, None)
        if event:
            event.set()


_cache = _Cache()


# ---------------------------------------------------------------------------
# 공통 헬퍼
# ---------------------------------------------------------------------------

def _tag_source(items: list, source: str) -> list:
    for item in items:
        item.data_source = source
    return items


# ---------------------------------------------------------------------------
# Power Grid Scraper
# ---------------------------------------------------------------------------

# ERCOT NP6-787-CD: 실시간 허브 평균 LMP ($/MWh → $/kWh 변환)
_ERCOT_LMP_URL = (
    "https://api.ercot.com/api/public-reports/np6-787-cd/spp_node_zone_hub"
)
_ERCOT_TOKEN_URL = (
    "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
)

# KEPCO / EirGrid: 공개 실시간 API 없음 → 현실적 시계열 시뮬레이션 사용
_KEPCO_FALLBACK = PowerPrice(
    region="kr-seoul", provider="KEPCO",
    price_usd_per_kwh=0.08, carbon_intensity_gco2_per_kwh=470.0,
    data_source="fallback",
)
_EIRGRID_FALLBACK = PowerPrice(
    region="ie-dublin", provider="EirGrid",
    price_usd_per_kwh=0.10, carbon_intensity_gco2_per_kwh=200.0,
    data_source="fallback",
)
_ERCOT_FALLBACK = PowerPrice(
    region="us-texas", provider="ERCOT",
    price_usd_per_kwh=0.04, is_negative=False, carbon_intensity_gco2_per_kwh=350.0,
    data_source="fallback",
)


def _get_ercot_token() -> Optional[str]:
    """ERCOT OIDC Bearer 토큰 획득 (Microsoft Entra ID)."""
    tenant_id  = os.environ.get("ERCOT_TENANT_ID", "")
    client_id  = os.environ.get("ERCOT_CLIENT_ID", "")
    client_sec = os.environ.get("ERCOT_CLIENT_SECRET", "")

    if not all([tenant_id, client_id, client_sec]):
        logger.debug("ERCOT credentials not set; skipping token fetch.")
        return None

    try:
        resp = requests.post(
            _ERCOT_TOKEN_URL.format(tenant_id=tenant_id),
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_sec,
                "scope":         "openid",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("id_token") or resp.json().get("access_token")
    except Exception as exc:
        logger.warning("ERCOT token fetch failed: %s", exc)
        return None


class PowerGridScraper:
    """
    글로벌 전력망 요금 스크레이퍼.

    ERCOT: 실제 Public API 연동 (ERCOT_SUBSCRIPTION_KEY 등 환경변수 필요)
    KEPCO / EirGrid: 공개 실시간 API 없음 → 시계열 기반 현실적 시뮬레이션
    """

    # ── KEPCO (시계열 시뮬레이션) ──────────────────────────────────────────

    def fetch_kepco(self) -> PowerPrice:
        """
        한국전력(KEPCO) 산업용 고압A 시간대별 차등 요금 시뮬레이션.
        공개 실시간 API 없음 — 실제 연동 시 KEPCO Open API 키 필요.
        """
        cache_key = "kepco"
        cached = _cache.get(cache_key)
        if cached:
            return cached

        # 스탬피드 방지: 첫 번째 스레드만 fetch, 나머지는 stale 즉시 반환
        stale = _cache.get_stale(cache_key)
        is_leader, wait_event = _cache.acquire_fetch_token(cache_key)
        if not is_leader:
            if stale:
                return stale
            wait_event.wait(timeout=5.0)
            return _cache.get(cache_key) or _KEPCO_FALLBACK

        try:
            now = datetime.now(timezone.utc)
            kst_hour = (now.hour + 9) % 24
            season_factor = 1.15 if now.month in (7, 8, 12, 1) else 1.0

            if 23 <= kst_hour or kst_hour < 9:
                base = 0.048
            elif kst_hour < 12 or kst_hour >= 18:
                base = 0.082
            else:
                base = 0.128

            price = round(base * season_factor, 4)
            result = PowerPrice(
                region="kr-seoul",
                provider="KEPCO",
                price_usd_per_kwh=price,
                carbon_intensity_gco2_per_kwh=round(420 + (kst_hour % 6) * 15, 1),
                data_source="simulation",
            )
            _cache.set(cache_key, result)
            logger.info("KEPCO [simulation] $%.4f/kWh (KST %02d:xx)", price, kst_hour)
            return result
        except Exception as exc:
            _cache.release_fetch_token(cache_key)
            logger.error("KEPCO fetch error: %s", exc)
            return stale or _KEPCO_FALLBACK

    # ── ERCOT (실제 API 연동) ─────────────────────────────────────────────

    def fetch_ercot(self) -> PowerPrice:
        """
        ERCOT 실시간 허브 평균 LMP 조회.

        필요 환경변수:
          ERCOT_SUBSCRIPTION_KEY  - ERCOT Developer Portal 구독 키
          ERCOT_TENANT_ID         - Microsoft Entra tenant ID
          ERCOT_CLIENT_ID         - 앱 등록 Client ID
          ERCOT_CLIENT_SECRET     - 앱 등록 Client Secret

        환경변수 미설정 시 → 시계열 시뮬레이션 후 반환.
        """
        cache_key = "ercot"
        cached = _cache.get(cache_key)
        if cached:
            return cached

        sub_key = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")
        if not sub_key:
            return self._ercot_simulation()

        token = _get_ercot_token()
        if not token:
            return self._ercot_fallback("token_error")

        try:
            resp = requests.get(
                _ERCOT_LMP_URL,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Ocp-Apim-Subscription-Key": sub_key,
                },
                params={"size": 1, "page": 1},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            # 응답 구조: {"data": [[..., lmp_value, ...], ...]}
            rows = data.get("data", [])
            if not rows:
                raise ValueError("Empty ERCOT response")

            # NP6-787-CD 컬럼: [deliveryDate, deliveryHour, deliveryInterval,
            #                    repetitionInterval, settlementPoint, lmp]
            lmp_mwh = float(rows[0][-1])
            price_kwh = round(lmp_mwh / 1000.0, 5)
            is_negative = price_kwh < 0

            result = PowerPrice(
                region="us-texas",
                provider="ERCOT",
                price_usd_per_kwh=max(price_kwh, -0.05),
                is_negative=is_negative,
                carbon_intensity_gco2_per_kwh=None,
                data_source="live",
            )
            _cache.set(cache_key, result)
            logger.info("ERCOT [live] LMP $%.5f/kWh (negative=%s)", price_kwh, is_negative)
            return result

        except requests.Timeout:
            logger.warning("ERCOT API timeout; using fallback.")
            return self._ercot_fallback("timeout")
        except requests.HTTPError as exc:
            logger.warning("ERCOT HTTP error %s; using fallback.", exc.response.status_code)
            return self._ercot_fallback(f"http_{exc.response.status_code}")
        except Exception as exc:
            logger.error("ERCOT unexpected error: %s", exc)
            return self._ercot_fallback("error")

    def _ercot_simulation(self) -> PowerPrice:
        """환경변수 미설정 시 시계열 기반 ERCOT 가격 시뮬레이션."""
        utc_hour = datetime.now(timezone.utc).hour
        if 7 <= utc_hour < 13:    # CDT 02:00–08:00 → 야간 풍력 과잉
            price = round(-0.005 + utc_hour * 0.001, 5)
            price = max(price, -0.02)
        elif 19 <= utc_hour < 25 or utc_hour < 1:  # CDT 14:00–20:00 피크
            price = round(0.085 + (utc_hour % 6) * 0.008, 4)
        else:
            price = round(0.035 + utc_hour * 0.0005, 4)

        is_negative = price < 0
        result = PowerPrice(
            region="us-texas", provider="ERCOT",
            price_usd_per_kwh=price, is_negative=is_negative,
            carbon_intensity_gco2_per_kwh=round(280 + utc_hour * 5, 1),
            data_source="simulation",
        )
        logger.info("ERCOT [simulation] $%.5f/kWh", price)
        return result

    def _ercot_fallback(self, reason: str) -> PowerPrice:
        stale = _cache.get_stale("ercot")
        if stale:
            stale.data_source = "cache"
            logger.info("ERCOT using stale cache (reason=%s)", reason)
            return stale
        logger.warning("ERCOT returning static fallback (reason=%s)", reason)
        return _ERCOT_FALLBACK

    # ── EirGrid (시계열 시뮬레이션) ───────────────────────────────────────

    def fetch_eirgrid(self) -> PowerPrice:
        """
        아일랜드(EirGrid) 탄소 연동 요금 시뮬레이션.
        EirGrid API(https://www.smartgriddashboard.com) 는 비공개 구조로 변경됨.
        실제 연동 시 EIRGRID_API_KEY 환경변수 추가 필요.
        """
        cache_key = "eirgrid"
        cached = _cache.get(cache_key)
        if cached:
            return cached

        utc_hour = datetime.now(timezone.utc).hour
        # 아일랜드 풍력 비중은 야간 높음 → 탄소 강도 낮음
        carbon = round(max(50, 320 - (utc_hour if utc_hour <= 12 else 24 - utc_hour) * 15), 1)
        base = 0.07 + (carbon - 50) / 3000
        carbon_surcharge = max(0.0, (carbon - 100) * 0.00005)
        price = round(base + carbon_surcharge, 4)

        result = PowerPrice(
            region="ie-dublin", provider="EirGrid",
            price_usd_per_kwh=price,
            carbon_intensity_gco2_per_kwh=carbon,
            data_source="simulation",
        )
        _cache.set(cache_key, result)
        logger.info("EirGrid [simulation] $%.4f/kWh (carbon=%.0f gCO2/kWh)", price, carbon)
        return result

    def fetch_all(self) -> list[PowerPrice]:
        base = [self.fetch_kepco(), self.fetch_ercot(), self.fetch_eirgrid()]
        eu = ENTSOEScraper().fetch_all()
        # ie-dublin은 EirGrid 시뮬레이션과 중복 → ENTSO-E 데이터가 live/cache면 덮어쓰기
        eu_regions = {p.region for p in eu}
        filtered_base = [p for p in base if p.region not in eu_regions or p.data_source == "fallback"]
        return filtered_base + eu


# ---------------------------------------------------------------------------
# Cloud Telemetry Scraper
# ---------------------------------------------------------------------------

# Lambda Labs Cloud API
_LAMBDA_BASE_URL = "https://cloud.lambdalabs.com/api/v1"

# H100 인스턴스 타입 → 리전 매핑 테이블 (Lambda Labs)
_LAMBDA_H100_TYPES = {
    "gpu_1x_h100_sxm5": "us-tx-3",
    "gpu_8x_h100_sxm5": "us-tx-3",
    "gpu_1x_h100_pcie": "us-az-1",
    "gpu_8x_h100_pcie": "us-az-1",
}

# AWS H100 인스턴스 타입 (p5.48xlarge = 8×H100 SXM5)
_AWS_H100_INSTANCE = "p5.48xlarge"
_AWS_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-2"]

# 정적 백업 데이터 (라이브 + 캐시 모두 실패 시)
_FALLBACK_INVENTORIES: list[CloudInventory] = [
    CloudInventory("ap-northeast-2", "AWS",        "H100", 8,  0.12, 13.50, 8.75,  data_source="fallback"),
    CloudInventory("us-east-1",      "AWS",        "H100", 20, 0.15, 12.30, 8.00,  data_source="fallback"),
    CloudInventory("us-west-2",      "AWS",        "H100", 15, 0.18, 12.30, 7.65,  data_source="fallback"),
    CloudInventory("eu-west-1",      "AWS",        "H100", 6,  0.22, 13.10, 8.90,  data_source="fallback"),
    CloudInventory("us-tx-3",        "LambdaLabs", "H100", 40, 0.05, 2.99,  2.39,  data_source="fallback"),
    CloudInventory("us-az-1",        "LambdaLabs", "H100", 25, 0.07, 2.99,  2.33,  data_source="fallback"),
    CloudInventory("eu-central-1",   "LambdaLabs", "H100", 12, 0.09, 3.29,  2.70,  data_source="fallback"),
]


class CloudTelemetryScraper:
    """
    AWS / Lambda Labs H100 가용 재고 및 스팟 가격 스크레이퍼.
    """

    # ── Lambda Labs ────────────────────────────────────────────────────────

    def fetch_lambda_labs(self) -> list[CloudInventory]:
        """
        Lambda Labs Cloud API GET /instance-types 를 호출하여
        H100 인스턴스의 가용 여부와 가격을 가져옵니다.

        필요 환경변수:
          LAMBDA_LABS_API_KEY  - Lambda Labs 대시보드에서 발급

        미설정 시 캐시 → 백업 정적 데이터 순서로 반환.
        """
        cache_key = "lambda_labs"
        cached = _cache.get(cache_key)
        if cached:
            return cached

        api_key = os.environ.get("LAMBDA_LABS_API_KEY", "")
        if not api_key:
            logger.warning("LAMBDA_LABS_API_KEY not set; using fallback.")
            return self._lambda_fallback("no_key")

        try:
            encoded = base64.b64encode(f"{api_key}:".encode()).decode()
            resp = requests.get(
                f"{_LAMBDA_BASE_URL}/instance-types",
                headers={"Authorization": f"Basic {encoded}"},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            raw: dict = resp.json().get("data", {})

            # 실제 인스턴스 이름 전체를 디버그 로그로 출력 (파싱 문제 진단용)
            all_names = list(raw.keys())
            logger.info("Lambda Labs instance types available: %s", all_names)

            # H100 관련 키워드: 공식 API는 'h100' 또는 'H100' 포함
            _GPU_KEYWORDS = ("h100", "h200", "a100", "a10", "v100")

            results: list[CloudInventory] = []
            for instance_type, info in raw.items():
                instance_lower = instance_type.lower()

                # GPU 모델 감지 — 이름에 키워드 포함 여부로 판단
                gpu_model = next(
                    (kw.upper() for kw in _GPU_KEYWORDS if kw in instance_lower),
                    None,
                )
                if gpu_model is None:
                    continue  # GPU 인스턴스가 아님

                # instance_type 중첩 구조 대응 (API 버전에 따라 다름)
                specs_obj   = info.get("instance_type") or info
                specs       = specs_obj.get("specs", {})
                price_cents = specs_obj.get("price_cents_per_hour", 0)
                price_usd   = round(price_cents / 100, 4)
                gpu_count   = specs.get("gpus", 1)

                regions_avail = info.get("regions_with_capacity_available", [])

                if regions_avail:
                    # 가용 리전마다 개별 레코드 생성
                    available = len(regions_avail) * gpu_count * 5
                    for region_info in regions_avail:
                        region_name = (
                            region_info.get("name")
                            or region_info.get("region", {}).get("name", "unknown")
                        )
                        results.append(CloudInventory(
                            region=region_name,
                            provider="LambdaLabs",
                            gpu_model=gpu_model,
                            available_units=available,
                            spot_revocation_probability=0.03,
                            on_demand_price_usd_per_hour=price_usd,
                            spot_price_usd_per_hour=price_usd,
                            data_source="live",
                        ))
                else:
                    # 현재 가용 재고 없음 — 재고 0으로 기록 (SSS에서 리스크 반영)
                    logger.debug("Lambda Labs %s: no regions with capacity.", instance_type)
                    results.append(CloudInventory(
                        region=f"lambda-{instance_type}",
                        provider="LambdaLabs",
                        gpu_model=gpu_model,
                        available_units=0,
                        spot_revocation_probability=0.03,
                        on_demand_price_usd_per_hour=price_usd,
                        spot_price_usd_per_hour=price_usd,
                        data_source="live",
                    ))

            if not results:
                logger.warning(
                    "Lambda Labs: no GPU instances matched. All types: %s", all_names
                )
                raise ValueError("No GPU instances found in Lambda Labs response")

            _cache.set(cache_key, results)
            logger.info("Lambda Labs [live] %d H100 region records fetched.", len(results))
            return results

        except requests.Timeout:
            logger.warning("Lambda Labs API timeout; using fallback.")
            return self._lambda_fallback("timeout")
        except requests.HTTPError as exc:
            logger.warning("Lambda Labs HTTP %s; using fallback.", exc.response.status_code)
            return self._lambda_fallback(f"http_{exc.response.status_code}")
        except Exception as exc:
            logger.error("Lambda Labs unexpected error: %s", exc)
            return self._lambda_fallback("error")

    def _lambda_fallback(self, reason: str) -> list[CloudInventory]:
        stale = _cache.get_stale("lambda_labs")
        if stale:
            logger.info("Lambda Labs using stale cache (reason=%s)", reason)
            return _tag_source(list(stale), "cache")
        logger.warning("Lambda Labs returning static fallback (reason=%s)", reason)
        return [i for i in _FALLBACK_INVENTORIES if i.provider == "LambdaLabs"]

    # ── AWS ───────────────────────────────────────────────────────────────

    def fetch_aws(self) -> list[CloudInventory]:
        """
        AWS boto3 describe_spot_price_history() 로 p5.48xlarge (H100 × 8)
        스팟 가격을 조회하고, 인스턴스 가용 수는 describe_instance_type_offerings
        로 추정합니다.

        필요 환경변수 (또는 ~/.aws/credentials / IAM Role):
          AWS_ACCESS_KEY_ID
          AWS_SECRET_ACCESS_KEY
          AWS_DEFAULT_REGION  (선택, 기본: us-east-1)

        boto3 미설치 또는 자격증명 없으면 캐시 → 백업 데이터 반환.
        """
        cache_key = "aws"
        cached = _cache.get(cache_key)
        if cached:
            return cached

        try:
            import boto3
            from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
        except ImportError:
            logger.warning("boto3 not installed; using AWS fallback. (pip install boto3)")
            return self._aws_fallback("no_boto3")

        results: list[CloudInventory] = []

        for region in _AWS_REGIONS:
            try:
                ec2 = boto3.client(
                    "ec2",
                    region_name=region,
                    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                )

                # ─ 온디맨드 가격 (AWS Price List 공개 JSON) ─
                on_demand = self._fetch_aws_ondemand_price(region)

                # ─ AZ별 스팟 가격 조회 (MaxResults 확대로 모든 AZ 커버) ─
                spot_resp = ec2.describe_spot_price_history(
                    InstanceTypes=[_AWS_H100_INSTANCE],
                    ProductDescriptions=["Linux/UNIX"],
                    MaxResults=20,
                )
                history = spot_resp.get("SpotPriceHistory", [])
                if not history:
                    logger.debug("AWS [%s] no spot history for %s", region, _AWS_H100_INSTANCE)
                    continue

                # 응답은 최신순 정렬 → AZ별 첫 번째 항목이 최신 가격
                az_spot: dict[str, float] = {}
                for entry in history:
                    az = entry.get("AvailabilityZone", "")
                    if az and az not in az_spot:
                        az_spot[az] = float(entry["SpotPrice"])

                if not az_spot:
                    continue

                # ─ 인스턴스 오퍼링이 있는 AZ 집합 (실제 구매 가능 여부 확인) ─
                offering_resp = ec2.describe_instance_type_offerings(
                    LocationType="availability-zone",
                    Filters=[{"Name": "instance-type", "Values": [_AWS_H100_INSTANCE]}],
                )
                offered_azs: set[str] = {
                    o["Location"]
                    for o in offering_resp.get("InstanceTypeOfferings", [])
                }

                # ─ AZ별 개별 CloudInventory 레코드 생성 ─
                for az, spot_price in az_spot.items():
                    if offered_azs and az not in offered_azs:
                        continue  # 해당 AZ에서 인스턴스 타입 미제공

                    ratio = spot_price / on_demand if on_demand else 0.5
                    revocation_prob = round(min(ratio * 0.6, 0.45), 3)

                    results.append(CloudInventory(
                        region=region,                   # 예: us-east-1
                        provider="AWS",
                        gpu_model="H100",
                        available_units=8,               # 보수적 추정: AZ당 p5.48xlarge 1대(8 GPU)
                        spot_revocation_probability=revocation_prob,
                        on_demand_price_usd_per_hour=round(on_demand, 4),
                        spot_price_usd_per_hour=round(spot_price, 4),
                        data_source="live",
                        availability_zone=az,            # 예: us-east-1a
                    ))
                    logger.info(
                        "AWS [live] %s (%s) spot=$%.3f/hr on-demand=$%.3f/hr",
                        region, az, spot_price, on_demand,
                    )

            except Exception as exc:  # NoCredentialsError, ClientError 등
                logger.warning("AWS [%s] error: %s", region, exc)

        if not results:
            return self._aws_fallback("no_data")

        _cache.set(cache_key, results)
        return results

    @staticmethod
    def _fetch_aws_ondemand_price(region: str) -> float:
        """
        AWS Price List 공개 JSON API로 p5.48xlarge 온디맨드 가격 조회.
        인증 불필요. 실패 시 알려진 참고 가격 반환.
        """
        _KNOWN_PRICES = {
            "us-east-1":      98.32,
            "us-west-2":      98.32,
            "eu-west-1":     104.00,
            "ap-northeast-2": 110.00,
        }

        try:
            # 전체 인덱스가 아닌 리전별 경량 파일 사용 (인증 불필요 공개 엔드포인트)
            light_url = (
                f"https://pricing.us-east-1.amazonaws.com"
                f"/offers/v1.0/aws/AmazonEC2/current/{region}/index.json"
            )
            resp = requests.get(light_url, timeout=15, stream=True)
            resp.raise_for_status()

            import json
            # 스트리밍으로 첫 4MB만 파싱 시도
            content = b""
            for chunk in resp.iter_content(chunk_size=65536):
                content += chunk
                if len(content) > 4 * 1024 * 1024:
                    break

            # p5.48xlarge 온디맨드 가격만 찾기 (정규식 대신 JSON 파싱)
            data = json.loads(content.decode("utf-8", errors="ignore"))
            for _, product in data.get("products", {}).items():
                attr = product.get("attributes", {})
                if (
                    attr.get("instanceType") == _AWS_H100_INSTANCE
                    and attr.get("tenancy") == "Shared"
                    and attr.get("operatingSystem") == "Linux"
                    and attr.get("capacitystatus") == "Used"
                ):
                    sku = product.get("sku", "")
                    terms = data.get("terms", {}).get("OnDemand", {})
                    sku_terms = terms.get(sku, {})
                    for _, term in sku_terms.items():
                        for _, dim in term.get("priceDimensions", {}).items():
                            price_str = dim.get("pricePerUnit", {}).get("USD", "0")
                            price = float(price_str)
                            if price > 0:
                                return price
        except Exception as exc:
            logger.debug("AWS Price List fetch failed (%s); using known price.", exc)

        return _KNOWN_PRICES.get(region, 98.32)

    def _aws_fallback(self, reason: str) -> list[CloudInventory]:
        stale = _cache.get_stale("aws")
        if stale:
            logger.info("AWS using stale cache (reason=%s)", reason)
            return _tag_source(list(stale), "cache")
        logger.warning("AWS returning static fallback (reason=%s)", reason)
        return [i for i in _FALLBACK_INVENTORIES if i.provider == "AWS"]

    # ── 통합 ──────────────────────────────────────────────────────────────

    def fetch_all(self) -> list[CloudInventory]:
        aws     = self.fetch_aws()
        lambda_ = self.fetch_lambda_labs()
        runpod  = RunPodScraper().fetch_runpod()
        all_records = aws + lambda_ + runpod

        # 커버되지 않은 리전은 정적 레코드로 보완
        regions_covered = {r.region for r in all_records}
        for fb in _FALLBACK_INVENTORIES:
            if fb.region not in regions_covered:
                all_records.append(fb)

        logger.info(
            "CloudTelemetryScraper total %d region records (aws=%d, lambda=%d, runpod=%d)",
            len(all_records), len(aws), len(lambda_), len(runpod),
        )
        return all_records


# ---------------------------------------------------------------------------
# RunPod Scraper
# ---------------------------------------------------------------------------

_RUNPOD_GRAPHQL_URL = "https://api.runpod.io/graphql"

# 추적할 GPU 키워드 (소문자)
_RUNPOD_GPU_KEYWORDS = ("h100", "h200", "a100", "b200", "a10", "l40")

# API 키 없이 또는 호출 실패 시 사용할 정적 백업 (2026년 기준 RunPod 공개 시세)
_RUNPOD_FALLBACK: list[CloudInventory] = [
    CloudInventory("runpod-us-h100", "RunPod", "H100 SXM5",  80, 0.04, 3.69, 2.49, data_source="fallback"),
    CloudInventory("runpod-us-a100", "RunPod", "A100 SXM4",  60, 0.05, 1.89, 1.19, data_source="fallback"),
    CloudInventory("runpod-eu-h100", "RunPod", "H100 SXM5",  40, 0.05, 3.99, 2.79, data_source="fallback"),
    CloudInventory("runpod-eu-a100", "RunPod", "A100 SXM4",  25, 0.06, 1.99, 1.29, data_source="fallback"),
]

_RUNPOD_GPU_QUERY = """{
  gpuTypes {
    id
    displayName
    memoryInGb
    secureCloud
    communityCloud
    securePrice
    communitySpotPrice
    communityPrice
  }
}"""


class RunPodScraper:
    """
    RunPod GPU 재고 · 가격 스크레이퍼.

    RunPod GraphQL API (api.runpod.io/graphql) 를 통해 GPU 타입별
    가격과 가용 여부를 조회합니다.
      - RUNPOD_API_KEY 가 없어도 공개 gpuTypes 쿼리는 동작합니다.
      - API 키 있으면 더 상세한 재고 / 컴플라이언스 데이터 접근 가능.

    필요 환경변수 (선택):
      RUNPOD_API_KEY  - 보다 상세한 데이터를 위한 선택적 Bearer 토큰
    """

    def fetch_runpod(self) -> list[CloudInventory]:
        cache_key = "runpod"
        cached = _cache.get(cache_key)
        if cached:
            return cached

        stale = _cache.get_stale(cache_key)
        is_leader, wait_event = _cache.acquire_fetch_token(cache_key)
        if not is_leader:
            if stale:
                return _tag_source(list(stale), "cache")
            if wait_event:
                wait_event.wait(timeout=10.0)
            return _cache.get(cache_key) or list(_RUNPOD_FALLBACK)

        api_key = os.environ.get("RUNPOD_API_KEY", "")
        headers: dict = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            resp = requests.post(
                _RUNPOD_GRAPHQL_URL,
                json={"query": _RUNPOD_GPU_QUERY},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            body = resp.json()

            # data.gpuTypes 가 있으면 errors는 부분 실패로 간주 (lowestPrice 등 인증 필드)
            gpu_types: list[dict] = (body.get("data") or {}).get("gpuTypes", [])
            if not gpu_types and "errors" in body:
                raise ValueError(f"RunPod GraphQL fatal error: {body['errors'][:1]}")
            logger.info("RunPod GPU types returned: %d", len(gpu_types))

            results: list[CloudInventory] = []
            for gpu in gpu_types:
                gpu_id    = (gpu.get("id") or "").lower()
                gpu_name  = gpu.get("displayName") or gpu_id

                # 고성능 GPU 키워드 매칭
                gpu_model = next(
                    (kw.upper() for kw in _RUNPOD_GPU_KEYWORDS if kw in gpu_id),
                    None,
                )
                if gpu_model is None:
                    continue

                secure_price = float(gpu.get("securePrice") or 0.0)
                comm_price   = float(gpu.get("communityPrice") or 0.0)
                spot_price   = float(gpu.get("communitySpotPrice") or 0.0)

                on_demand = secure_price or comm_price
                spot      = spot_price or on_demand * 0.68
                if on_demand == 0 and spot == 0:
                    continue

                has_secure    = bool(gpu.get("secureCloud"))
                has_community = bool(gpu.get("communityCloud"))
                available_est = 100 if (has_secure or has_community) else 0
                revoc_prob    = 0.04 if has_community else 0.02

                # RunPod은 글로벌 단일가 → US / EU 두 리전으로 분류
                for region_id, price_mult in [
                    (f"runpod-us-{gpu_model.lower()}", 1.00),
                    (f"runpod-eu-{gpu_model.lower()}", 1.08),
                ]:
                    results.append(CloudInventory(
                        region=region_id,
                        provider="RunPod",
                        gpu_model=f"{gpu_model} ({gpu_name})",
                        available_units=available_est,
                        spot_revocation_probability=revoc_prob,
                        on_demand_price_usd_per_hour=round(on_demand * price_mult, 4),
                        spot_price_usd_per_hour=round(spot * price_mult, 4),
                        data_source="live",
                    ))

            if not results:
                logger.warning("RunPod: no GPU matched keywords; using fallback.")
                _cache.release_fetch_token(cache_key)
                return self._fallback("no_match", stale)

            # 동일 region_id → 최저 spot 가격 레코드만 보존
            best: dict[str, CloudInventory] = {}
            for inv in results:
                if inv.region not in best or inv.spot_price_usd_per_hour < best[inv.region].spot_price_usd_per_hour:
                    best[inv.region] = inv
            results = list(best.values())

            _cache.set(cache_key, results)
            logger.info("RunPod [live] %d region records cached.", len(results))
            return results

        except requests.Timeout:
            logger.warning("RunPod API timeout; using fallback.")
            _cache.release_fetch_token(cache_key)
            return self._fallback("timeout", stale)
        except Exception as exc:
            logger.error("RunPod unexpected error: %s", exc)
            _cache.release_fetch_token(cache_key)
            return self._fallback("error", stale)

    def _fallback(self, reason: str, stale=None) -> list[CloudInventory]:
        if stale:
            logger.info("RunPod using stale cache (reason=%s)", reason)
            return _tag_source(list(stale), "cache")
        logger.warning("RunPod returning static fallback (reason=%s)", reason)
        return list(_RUNPOD_FALLBACK)


# ---------------------------------------------------------------------------
# ENTSO-E Scraper (유럽 전력 도매가)
# ---------------------------------------------------------------------------

_ENTSOE_API_URL = "https://web-api.tp.entsoe.eu/api"
_ENTSOE_NS      = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"

# EIC 입찰 구역 코드 → (cloud_region_id, 표시명)
_ENTSOE_ZONES: dict[str, tuple[str, str]] = {
    "10YFR-RTE------C": ("eu-west-3",    "France (Paris)"),
    "10Y1001A1001A83F": ("eu-central-1", "Germany (Frankfurt)"),
    "10YNL----------L": ("eu-west-1",    "Netherlands"),
    "10Y1001A1001A47J": ("eu-north-1",   "Sweden"),
    "10Y1001A1001A59C": ("ie-dublin",    "Ireland"),
}

EUR_TO_USD = 1.08  # EUR → USD 환율 근삿값

# 정적 백업 (EUR 평균 시장가 기준)
_ENTSOE_FALLBACK: dict[str, PowerPrice] = {
    "eu-west-3":    PowerPrice("eu-west-3",    "ENTSO-E/France",       0.1080, data_source="fallback"),
    "eu-central-1": PowerPrice("eu-central-1", "ENTSO-E/Germany",      0.0950, data_source="fallback"),
    "eu-west-1":    PowerPrice("eu-west-1",    "ENTSO-E/Netherlands",  0.1020, data_source="fallback"),
    "eu-north-1":   PowerPrice("eu-north-1",   "ENTSO-E/Sweden",       0.0420, data_source="fallback"),
    "ie-dublin":    PowerPrice("ie-dublin",    "ENTSO-E/Ireland",      0.1150, data_source="fallback"),
}


class ENTSOEScraper:
    """
    ENTSO-E Transparency Platform 실시간 전력 도매가 스크레이퍼.
    EU 5개 권역(프랑스/독일/네덜란드/스웨덴/아일랜드) 당일 예측 가격 조회.

    필요 환경변수:
      ENTSOE_API_KEY  - ENTSO-E Transparency Platform 계정에서 발급
                        (https://transparency.entsoe.eu → 회원가입 → API 토큰 발급)

    미설정 시 → 현실적 EU 전력 시계열 시뮬레이션으로 대체됩니다.
    """

    def fetch_all(self) -> list[PowerPrice]:
        api_key = os.environ.get("ENTSOE_API_KEY", "")
        if not api_key:
            logger.info("ENTSOE_API_KEY not set; using EU time-series simulation.")
            return self._simulate_all()
        return [
            self._fetch_zone(zone_code, region_id, label, api_key)
            for zone_code, (region_id, label) in _ENTSOE_ZONES.items()
        ]

    # ------------------------------------------------------------------

    def _fetch_zone(
        self, zone_code: str, region_id: str, label: str, api_key: str
    ) -> PowerPrice:
        cache_key = f"entsoe_{zone_code}"
        cached = _cache.get(cache_key)
        if cached:
            return cached

        stale = _cache.get_stale(cache_key)
        is_leader, wait_event = _cache.acquire_fetch_token(cache_key)
        if not is_leader:
            if stale:
                import copy; r = copy.copy(stale); r.data_source = "cache"; return r
            if wait_event:
                wait_event.wait(timeout=10.0)
            return _cache.get(cache_key) or self._zone_fallback(zone_code, region_id)

        try:
            import xml.etree.ElementTree as ET
            from datetime import timedelta

            now    = datetime.now(timezone.utc)
            p_start = (now - timedelta(hours=1)).strftime("%Y%m%d%H00")
            p_end   = (now + timedelta(hours=2)).strftime("%Y%m%d%H00")

            resp = requests.get(
                _ENTSOE_API_URL,
                params={
                    "securityToken": api_key,
                    "documentType":  "A44",
                    "in_Domain":     zone_code,
                    "out_Domain":    zone_code,
                    "periodStart":   p_start,
                    "periodEnd":     p_end,
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()

            root = ET.fromstring(resp.content)
            ns   = {"ns": _ENTSOE_NS}

            prices: list[float] = []
            for ts in root.findall("ns:TimeSeries", ns):
                for period in ts.findall("ns:Period", ns):
                    for point in period.findall("ns:Point", ns):
                        price_el = point.find("ns:price.amount", ns)
                        if price_el is not None and price_el.text:
                            prices.append(float(price_el.text))

            if not prices:
                raise ValueError(f"No price points in ENTSO-E response for {zone_code}")

            # 현재 시간의 position 인덱스 (1-indexed positions, 첫 번째가 직전 시간)
            price_eur_mwh = prices[min(1, len(prices) - 1)]
            price_usd_kwh = round(price_eur_mwh * EUR_TO_USD / 1000.0, 6)

            result = PowerPrice(
                region=region_id,
                provider=f"ENTSO-E/{label.split()[0]}",
                price_usd_per_kwh=price_usd_kwh,
                is_negative=price_usd_kwh < 0,
                data_source="live",
            )
            _cache.set(cache_key, result)
            logger.info(
                "ENTSO-E [live] %-30s €%.2f/MWh → $%.5f/kWh",
                label, price_eur_mwh, price_usd_kwh,
            )
            return result

        except requests.Timeout:
            logger.warning("ENTSO-E timeout for %s", label)
            _cache.release_fetch_token(cache_key)
            return self._zone_fallback(zone_code, region_id, stale=stale)
        except Exception as exc:
            logger.error("ENTSO-E error for %s: %s", label, exc)
            _cache.release_fetch_token(cache_key)
            return self._zone_fallback(zone_code, region_id, stale=stale)

    def _zone_fallback(
        self, zone_code: str, region_id: str, stale=None
    ) -> PowerPrice:
        import copy
        if stale:
            r = copy.copy(stale); r.data_source = "cache"; return r
        fb = _ENTSOE_FALLBACK.get(region_id)
        if fb:
            return copy.copy(fb)
        return copy.copy(_EIRGRID_FALLBACK)

    # ------------------------------------------------------------------

    def _simulate_all(self) -> list[PowerPrice]:
        """ENTSO-E API 키 없을 때 EU 전력 시계열 시뮬레이션."""
        import copy
        utc_hour = datetime.now(timezone.utc).hour
        # 전력 수요는 오전 8시~오후 8시 피크
        peak = 1.0 + 0.6 * max(0.0, 1.0 - abs(utc_hour - 13) / 6.0)

        configs = [
            # (region_id, provider, base_eur_mwh, carbon_min, carbon_max)
            ("eu-west-3",    "ENTSO-E/France",       55.0, 20,  60),   # 원전 많아 탄소 낮음
            ("eu-central-1", "ENTSO-E/Germany",      70.0, 80, 280),   # 석탄+재생에너지 혼재
            ("eu-west-1",    "ENTSO-E/Netherlands",  68.0, 60, 230),   # 가스 의존
            ("eu-north-1",   "ENTSO-E/Sweden",       25.0,  5,  30),   # 수력/원전 → 최저가
            ("ie-dublin",    "ENTSO-E/Ireland",      72.0, 50, 300),   # 풍력 비중 높음
        ]

        results: list[PowerPrice] = []
        for region_id, provider, base_eur, c_min, c_max in configs:
            eur_mwh   = round(base_eur * peak + (utc_hour % 6) * 1.5, 2)
            usd_kwh   = round(eur_mwh * EUR_TO_USD / 1000.0, 6)
            carbon    = round(c_min + (c_max - c_min) * (peak - 1.0) / 0.6, 1)
            carbon    = max(c_min, min(c_max, carbon))
            results.append(PowerPrice(
                region=region_id,
                provider=provider,
                price_usd_per_kwh=max(usd_kwh, 0.001),
                carbon_intensity_gco2_per_kwh=carbon,
                data_source="simulation",
            ))
            logger.debug("ENTSO-E [sim] %-20s €%.1f/MWh → $%.5f/kWh", provider, eur_mwh, usd_kwh)

        return results
