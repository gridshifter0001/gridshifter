"""
stripe_billing.py — GridShifter AI 결제 시스템
Stripe 연동: 구독 플랜 관리, Checkout 세션, Webhook 처리

플랜 구조
  free  : 대시보드 조회 무제한 / CLI dry-run 무제한 / 실제 배포 월 3회
  pro   : 실제 배포 무제한 / 자동 종료 / JSON 저장 ($19/월)

베타 운영 전략
  BILLING_ENABLED=false (기본) → 모든 기능 무료 오픈
  BILLING_ENABLED=true        → 플랜별 제한 적용
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("gridshifter.billing")

# ── Stripe 키 (환경변수) ────────────────────────────────────────────────────
STRIPE_SECRET_KEY      = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET  = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRO_PRICE_ID    = os.environ.get("STRIPE_PRO_PRICE_ID", "")   # Stripe Dashboard에서 생성

# ── 과금 활성화 스위치 (false = 베타 무료 오픈) ────────────────────────────
BILLING_ENABLED = os.environ.get("BILLING_ENABLED", "false").lower() == "true"

# ── 플랜 정의 ───────────────────────────────────────────────────────────────
PLANS: dict[str, dict] = {
    "free": {
        "name":           "Free",
        "price_usd":      0,
        "deployments_per_month": 3,
        "features": [
            "대시보드 조회 무제한",
            "CLI dry-run 무제한",
            "실제 배포 월 3회",
            "이메일 지원",
        ],
    },
    "pro": {
        "name":           "Pro",
        "price_usd":      19,
        "deployments_per_month": -1,   # 무제한
        "features": [
            "Free 플랜 전체 포함",
            "실제 배포 무제한",
            "자동 종료 타이머",
            "배포 결과 JSON 저장",
            "RunPod + Lambda Labs 동시 비교",
            "우선 지원",
        ],
    },
}


# ── 간이 API Key 스토어 (프로덕션에서는 DB로 교체) ──────────────────────────
@dataclass
class ApiKeyRecord:
    key:        str
    email:      str
    plan:       str = "free"
    created_at: float = field(default_factory=time.time)
    deploy_count_this_month: int = 0
    stripe_customer_id: Optional[str] = None
    stripe_subscription_id: Optional[str] = None


# 인메모리 스토어 (서버 재시작 시 초기화 — 추후 Redis/DB로 교체)
_key_store: dict[str, ApiKeyRecord] = {}


def generate_api_key(email: str) -> str:
    """이메일 기반 API 키 생성 및 저장."""
    raw = f"gs_{email}_{time.time()}"
    key = "gs_" + hashlib.sha256(raw.encode()).hexdigest()[:32]
    _key_store[key] = ApiKeyRecord(key=key, email=email)
    logger.info("API key generated for %s", email)
    return key


def get_key_record(api_key: str) -> Optional[ApiKeyRecord]:
    return _key_store.get(api_key)


def can_deploy(api_key: Optional[str]) -> tuple[bool, str]:
    """
    배포 가능 여부 확인.
    Returns (allowed: bool, reason: str)
    """
    if not BILLING_ENABLED:
        return True, "beta_open"

    if not api_key:
        return False, "API 키가 필요합니다. /api/v1/billing/register 에서 발급받으세요."

    rec = get_key_record(api_key)
    if not rec:
        return False, "유효하지 않은 API 키입니다."

    if rec.plan == "pro":
        return True, "pro"

    # Free 플랜: 월 3회 제한
    if rec.deploy_count_this_month >= PLANS["free"]["deployments_per_month"]:
        return False, (
            f"Free 플랜 월 배포 한도({PLANS['free']['deployments_per_month']}회)를 초과했습니다. "
            "Pro로 업그레이드하세요."
        )

    return True, "free"


def record_deployment(api_key: str) -> None:
    """배포 카운트 증가."""
    rec = get_key_record(api_key)
    if rec:
        rec.deploy_count_this_month += 1


# ── Stripe 연동 ─────────────────────────────────────────────────────────────

def create_checkout_session(email: str, api_key: str, success_url: str, cancel_url: str) -> str:
    """
    Stripe Checkout 세션을 생성하고 URL을 반환합니다.
    BILLING_ENABLED=false 이면 빈 문자열 반환 (베타 무료).
    """
    if not BILLING_ENABLED:
        return ""

    if not STRIPE_SECRET_KEY:
        raise RuntimeError("STRIPE_SECRET_KEY가 설정되지 않았습니다.")

    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY

        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            customer_email=email,
            line_items=[{
                "price":    STRIPE_PRO_PRICE_ID,
                "quantity": 1,
            }],
            metadata={"api_key": api_key},
            success_url=success_url + "?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=cancel_url,
        )
        return session.url
    except Exception as exc:
        logger.error("Stripe checkout session error: %s", exc)
        raise


def handle_webhook(payload: bytes, sig_header: str) -> dict:
    """
    Stripe Webhook 이벤트를 처리합니다.
    결제 완료 시 → 해당 API 키의 플랜을 pro로 업그레이드.
    """
    if not STRIPE_WEBHOOK_SECRET:
        raise RuntimeError("STRIPE_WEBHOOK_SECRET이 설정되지 않았습니다.")

    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception as exc:
        raise ValueError(f"Webhook 서명 검증 실패: {exc}")

    event_type = event["type"]
    logger.info("Stripe webhook: %s", event_type)

    if event_type == "checkout.session.completed":
        session    = event["data"]["object"]
        api_key    = session.get("metadata", {}).get("api_key", "")
        customer   = session.get("customer", "")
        sub_id     = session.get("subscription", "")
        rec = get_key_record(api_key)
        if rec:
            rec.plan = "pro"
            rec.stripe_customer_id     = customer
            rec.stripe_subscription_id = sub_id
            logger.info("Upgraded to Pro: %s (customer=%s)", rec.email, customer)

    elif event_type in ("customer.subscription.deleted", "customer.subscription.paused"):
        sub = event["data"]["object"]
        # 구독 취소 → free로 다운그레이드
        for rec in _key_store.values():
            if rec.stripe_subscription_id == sub["id"]:
                rec.plan = "free"
                logger.info("Downgraded to Free: %s", rec.email)
                break

    return {"status": "ok", "event": event_type}


def get_plan_info(api_key: Optional[str]) -> dict:
    """현재 플랜 정보 반환."""
    if not BILLING_ENABLED:
        return {
            "plan":    "beta",
            "billing": "disabled",
            "message": "베타 기간 전 기능 무료 오픈 중",
            "features": PLANS["pro"]["features"],
        }

    if not api_key:
        return {"plan": "anonymous", **PLANS["free"]}

    rec = get_key_record(api_key)
    if not rec:
        return {"plan": "invalid"}

    plan_data = PLANS.get(rec.plan, PLANS["free"]).copy()
    plan_data["email"]        = rec.email
    plan_data["deploy_count"] = rec.deploy_count_this_month
    return plan_data
