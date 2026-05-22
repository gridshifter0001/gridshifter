"""
feedback.py — GridShifter AI 배포 결과 피드백 수집

유저의 명시적 동의 없이 개인 정보를 수집하지 않습니다.
수집 항목: 추천 리전, 실제 채택 여부, 예상 비용 vs 실제 비용, 타임스탬프
식별자: 익명 세션 해시 (IP + UA의 단방향 해시, 역추적 불가)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import Lock
from typing import Optional

logger = logging.getLogger("gridshifter.feedback")

_FEEDBACK_FILE = Path(os.environ.get("FEEDBACK_PATH", "feedback_log.jsonl"))
_lock = Lock()


@dataclass
class FeedbackEvent:
    event_type: str                    # "recommendation" | "adoption" | "result"
    timestamp: float = field(default_factory=time.time)
    session_id: str = ""               # 익명 해시
    recommended_region: str = ""
    adopted: Optional[bool] = None     # True=채택, False=거부, None=미응답
    estimated_cost_usd: float = 0.0
    actual_cost_usd: float = 0.0       # CLI --save 결과에서 수집
    gpu_count: int = 0
    job_hours: float = 0.0
    workload_type: str = ""
    dataset_gb: float = 0.0
    egress_cost_usd: float = 0.0
    provider: str = ""
    savings_pct: float = 0.0
    sss_score: float = 0.0


def _session_id(ip: str, user_agent: str) -> str:
    """IP + UA → 단방향 해시 (역추적 불가)."""
    raw = f"{ip}:{user_agent}:{time.strftime('%Y-%m-%d')}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def record(event: FeedbackEvent) -> None:
    """피드백 이벤트를 JSONL 파일에 추가."""
    try:
        with _lock:
            with _FEEDBACK_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("Feedback write failed: %s", exc)


def get_stats() -> dict:
    """
    수집된 피드백 통계 반환.
    - 총 추천 수
    - 채택률
    - 평균 절감률
    - 가장 많이 채택된 리전 Top 5
    - 이그레스 경고 발생률
    """
    if not _FEEDBACK_FILE.exists():
        return {"total": 0, "message": "아직 수집된 데이터가 없습니다."}

    events: list[dict] = []
    try:
        with _FEEDBACK_FILE.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
    except Exception as exc:
        logger.warning("Feedback read failed: %s", exc)
        return {"total": 0, "error": str(exc)}

    recs    = [e for e in events if e.get("event_type") == "recommendation"]
    adopted = [e for e in recs if e.get("adopted") is True]

    region_counts: dict[str, int] = {}
    for e in adopted:
        r = e.get("recommended_region", "")
        if r:
            region_counts[r] = region_counts.get(r, 0) + 1

    top_regions = sorted(region_counts.items(), key=lambda x: -x[1])[:5]

    avg_savings = (
        sum(e.get("savings_pct", 0) for e in adopted) / len(adopted)
        if adopted else 0
    )
    egress_warned = sum(1 for e in recs if e.get("egress_cost_usd", 0) > 0)

    return {
        "total_recommendations": len(recs),
        "adoption_count":        len(adopted),
        "adoption_rate_pct":     round(len(adopted) / max(len(recs), 1) * 100, 1),
        "avg_savings_pct":       round(avg_savings, 1),
        "top_adopted_regions":   [{"region": r, "count": c} for r, c in top_regions],
        "egress_warning_count":  egress_warned,
        "data_since":            (
            time.strftime("%Y-%m-%d", time.localtime(min(e["timestamp"] for e in events)))
            if events else None
        ),
    }
