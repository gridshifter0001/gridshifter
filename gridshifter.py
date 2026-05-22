#!/usr/bin/env python3
"""
gridshifter.py — GridShifter AI Phase 2a CLI
최적 가성비 클라우드 리전 탐색 + 인스턴스 자동 생성

사용법:
  python gridshifter.py run --image pytorch/pytorch:latest --gpu H100 --count 4 --hours 8
  python gridshifter.py run --image nvcr.io/nvidia/cuda:12-devel --dry-run
  python gridshifter.py status
"""

from __future__ import annotations

import argparse
import atexit
import io
import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Windows 콘솔 UTF-8 강제 (cp949 → utf-8)
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests
from dotenv import load_dotenv

# ── 환경변수 로드 ──────────────────────────────────────────────────────────────
_ENV_FILE = Path(__file__).parent / ".env.txt"
if _ENV_FILE.exists():
    load_dotenv(dotenv_path=_ENV_FILE, override=False)

# ── 상수 ──────────────────────────────────────────────────────────────────────
GRIDSHIFTER_API = os.environ.get("GRIDSHIFTER_API", "http://localhost:8000")
RUNPOD_API_KEY  = os.environ.get("RUNPOD_API_KEY", "")
LAMBDA_API_KEY  = os.environ.get("LAMBDA_LABS_API_KEY", "")

RUNPOD_GQL_URL  = "https://api.runpod.io/graphql"
LAMBDA_BASE_URL = "https://cloud.lambdalabs.com/api/v1"

REQUEST_TIMEOUT = 30   # 외부 API 타임아웃 (초)
POLL_INTERVAL   = 10   # 인스턴스 상태 폴링 간격 (초)
POLL_MAX_WAIT   = 300  # 최대 대기 시간 (5분)

# ── ANSI 색상 ──────────────────────────────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    RED    = "\033[91m"
    CYAN   = "\033[96m"
    BLUE   = "\033[94m"
    GREY   = "\033[90m"
    WHITE  = "\033[97m"

def ok(msg: str)   -> None: print(f"{C.GREEN}  ✓{C.RESET} {msg}")
def warn(msg: str) -> None: print(f"{C.YELLOW}  ⚠{C.RESET} {msg}")
def err(msg: str)  -> None: print(f"{C.RED}  ✗{C.RESET} {msg}", file=sys.stderr)
def info(msg: str) -> None: print(f"{C.GREY}    {msg}{C.RESET}")
def step(msg: str) -> None: print(f"\n{C.CYAN}{C.BOLD}▶ {msg}{C.RESET}")
def banner(msg: str) -> None:
    w = 60
    print(f"\n{C.BLUE}{'─'*w}{C.RESET}")
    print(f"{C.BLUE}{C.BOLD}  {msg}{C.RESET}")
    print(f"{C.BLUE}{'─'*w}{C.RESET}")

# ── GPU 타입 매핑 ──────────────────────────────────────────────────────────────
# (CLI 입력 → RunPod GPU Type ID / Lambda instance type)
_GPU_MAP: dict[str, dict] = {
    "H100": {
        "runpod_id":    "NVIDIA H100 80GB HBM3",
        "lambda_type":  "gpu_1x_h100_sxm5",
        "display":      "H100 SXM 80GB",
        "approx_usd_hr": 2.69,
    },
    "H100-PCIE": {
        "runpod_id":    "NVIDIA H100 PCIe",
        "lambda_type":  "gpu_1x_h100_pcie",
        "display":      "H100 PCIe 80GB",
        "approx_usd_hr": 1.99,
    },
    "H200": {
        "runpod_id":    "NVIDIA H200",
        "lambda_type":  "gpu_1x_gh200",
        "display":      "H200 SXM 141GB",
        "approx_usd_hr": 3.59,
    },
    "A100": {
        "runpod_id":    "NVIDIA A100 80GB PCIe",
        "lambda_type":  "gpu_1x_a100_80gb_sxm4",
        "display":      "A100 PCIe 80GB",
        "approx_usd_hr": 1.19,
    },
    "A100-SXM": {
        "runpod_id":    "NVIDIA A100-SXM4-80GB",
        "lambda_type":  "gpu_1x_a100_80gb_sxm4",
        "display":      "A100 SXM4 80GB",
        "approx_usd_hr": 1.39,
    },
    "L40": {
        "runpod_id":    "NVIDIA L40",
        "lambda_type":  "gpu_1x_rtx6000",
        "display":      "L40 48GB",
        "approx_usd_hr": 0.69,
    },
    "L40S": {
        "runpod_id":    "NVIDIA L40S",
        "lambda_type":  "gpu_1x_rtx6000",
        "display":      "L40S 48GB",
        "approx_usd_hr": 0.79,
    },
    "RTX4090": {
        "runpod_id":    "NVIDIA GeForce RTX 4090",
        "lambda_type":  "gpu_1x_rtx6000",
        "display":      "RTX 4090 24GB",
        "approx_usd_hr": 0.34,
    },
    "A40": {
        "runpod_id":    "NVIDIA A40",
        "lambda_type":  "gpu_1x_a10",
        "display":      "A40 48GB",
        "approx_usd_hr": 0.35,
    },
    "A5000": {
        "runpod_id":    "NVIDIA RTX A5000",
        "lambda_type":  "gpu_1x_a10",
        "display":      "RTX A5000 24GB",
        "approx_usd_hr": 0.16,
    },
    "RTX3090": {
        "runpod_id":    "NVIDIA GeForce RTX 3090",
        "lambda_type":  "gpu_1x_a10",
        "display":      "RTX 3090 24GB",
        "approx_usd_hr": 0.22,
    },
}

# ── 전역 종료 핸들러 (비용 사고 방지) ──────────────────────────────────────────
_active_instance: dict = {}   # {"provider": ..., "id": ..., "name": ...}

def _cleanup_on_exit() -> None:
    """프로그램 종료 시 인스턴스 자동 종료 (--auto-terminate 플래그 ON 시)."""
    if not _active_instance.get("auto_terminate"):
        return
    provider = _active_instance.get("provider", "")
    inst_id  = _active_instance.get("id", "")
    if not inst_id:
        return
    warn(f"자동 종료 실행 중: {provider} / {inst_id}")
    try:
        if provider == "RunPod":
            _runpod_terminate(inst_id)
        elif provider == "LambdaLabs":
            _lambda_terminate(inst_id)
        ok("인스턴스 종료 완료")
    except Exception as exc:
        err(f"자동 종료 실패 (수동으로 종료하세요): {exc}")

atexit.register(_cleanup_on_exit)

def _handle_sigint(sig, frame) -> None:
    print()
    warn("Ctrl+C 감지 — 정리 중…")
    sys.exit(0)

signal.signal(signal.SIGINT, _handle_sigint)


# ═══════════════════════════════════════════════════════════════════════════════
# GridShifter 백엔드 호출
# ═══════════════════════════════════════════════════════════════════════════════

def call_gridshifter(
    gpu_model:  str,
    gpu_count:  int,
    job_hours:  float,
    region:     str,
    workload:   str,
) -> dict:
    """
    GridShifter 백엔드 /api/v1/optimize 를 호출해 최적 리전 추천을 받습니다.
    """
    payload = {
        "current_region":         region,
        "gpu_spec":               {"model": gpu_model, "count": gpu_count},
        "job_duration_hours":     job_hours,
        "require_data_compliance": False,
        "workload_type":          workload,
        "inbound_ports":          [],
        "latency_risk_acknowledged": True,   # CLI는 배치 기본 → 글로벌 허용
        "stateless":              True,
    }
    try:
        resp = requests.post(
            f"{GRIDSHIFTER_API}/api/v1/optimize",
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.ConnectionError:
        raise RuntimeError(
            f"GridShifter 백엔드({GRIDSHIFTER_API})에 연결할 수 없습니다.\n"
            "    서버가 실행 중인지 확인: python app.py"
        )
    except requests.HTTPError as exc:
        body = exc.response.json() if exc.response else {}
        raise RuntimeError(f"백엔드 오류 ({exc.response.status_code}): {body.get('detail', exc)}")


# ═══════════════════════════════════════════════════════════════════════════════
# RunPod 핸들러
# ═══════════════════════════════════════════════════════════════════════════════

def _runpod_gql(query: str, variables: Optional[dict] = None) -> dict:
    """RunPod GraphQL 호출 공통 래퍼."""
    if not RUNPOD_API_KEY:
        raise RuntimeError("RUNPOD_API_KEY가 설정되지 않았습니다. .env.txt를 확인하세요.")
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
    }
    body: dict = {"query": query}
    if variables:
        body["variables"] = variables
    resp = requests.post(RUNPOD_GQL_URL, json=body, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    # GraphQL 에러 중 data가 있으면 부분 성공으로 허용
    if "errors" in data and not data.get("data"):
        raise RuntimeError(f"RunPod GraphQL 오류: {data['errors'][0]['message']}")
    return data.get("data", {})


def runpod_launch(
    image:      str,
    gpu_type_id: str,
    gpu_count:  int,
    job_name:   str,
    docker_cmd: str = "",
    ports:      str = "22/tcp",
) -> str:
    """
    RunPod에 Pod를 생성하고 Pod ID를 반환합니다.
    SSH 없이 RunPod이 직접 컨테이너를 실행합니다.
    """
    mutation = """
    mutation LaunchPod($input: PodFindAndDeployOnDemandInput!) {
      podFindAndDeployOnDemand(input: $input) {
        id
        name
        imageName
        machineId
        desiredStatus
      }
    }
    """
    variables = {
        "input": {
            "cloudType":        "ALL",
            "gpuCount":         gpu_count,
            "volumeInGb":       0,
            "containerDiskInGb": 20,
            "minMemoryInGb":    15,
            "gpuTypeId":        gpu_type_id,
            "name":             job_name,
            "imageName":        image,
            "dockerArgs":       docker_cmd,
            "ports":            ports,
            "volumeMountPath":  "/workspace",
            "env":              [],
        }
    }
    raw = _runpod_gql(mutation, variables)
    pod = raw.get("podFindAndDeployOnDemand")
    if not pod:
        # GraphQL errors 필드에 상세 원인이 있을 수 있음
        raise RuntimeError(
            f"RunPod pod 생성 실패: 응답 없음. "
            f"원인: GPU 재고 부족(SUPPLY_CONSTRAINT) 또는 잔액 부족일 수 있습니다. "
            f"RunPod 대시보드(runpod.io)에서 {gpu_type_id} 재고를 확인하세요."
        )
    if not pod.get("id"):
        raise RuntimeError(f"RunPod pod 생성 실패: ID 없음. 응답: {pod}")
    return pod["id"]


def runpod_get_ip(pod_id: str) -> Optional[str]:
    """RunPod pod의 공개 IP를 반환합니다 (준비 안 됐으면 None)."""
    query = """
    query GetPod($input: PodFilter!) {
      pod(input: $input) {
        id
        desiredStatus
        runtime {
          uptimeInSeconds
          ports {
            ip
            isIpPublic
            privatePort
            publicPort
            type
          }
        }
      }
    }
    """
    data = _runpod_gql(query, {"input": {"podId": pod_id}})
    pod  = data.get("pod", {})
    runtime = pod.get("runtime") or {}
    ports   = runtime.get("ports") or []
    for p in ports:
        if p.get("isIpPublic") and p.get("ip"):
            return p["ip"]
    return None


def _runpod_terminate(pod_id: str) -> None:
    """RunPod pod를 종료합니다."""
    mutation = """
    mutation TerminatePod($input: PodTerminateInput!) {
      podTerminate(input: $input)
    }
    """
    _runpod_gql(mutation, {"input": {"podId": pod_id}})


# ═══════════════════════════════════════════════════════════════════════════════
# Lambda Labs 핸들러
# ═══════════════════════════════════════════════════════════════════════════════

def _lambda_req(method: str, path: str, **kwargs) -> dict:
    """Lambda Labs REST API 공통 래퍼."""
    if not LAMBDA_API_KEY:
        raise RuntimeError("LAMBDA_LABS_API_KEY가 설정되지 않았습니다. .env.txt를 확인하세요.")
    resp = requests.request(
        method,
        f"{LAMBDA_BASE_URL}{path}",
        auth=(LAMBDA_API_KEY, ""),
        timeout=REQUEST_TIMEOUT,
        **kwargs,
    )
    resp.raise_for_status()
    return resp.json()


def lambda_get_ssh_keys() -> list[str]:
    """Lambda Labs 계정에 등록된 SSH 키 이름 목록을 반환합니다."""
    data = _lambda_req("GET", "/ssh-keys")
    return [k["name"] for k in data.get("data", [])]


def lambda_launch(
    instance_type: str,
    region_name:   str,
    ssh_key_name:  str,
    job_name:      str,
) -> str:
    """
    Lambda Labs 인스턴스를 생성하고 Instance ID를 반환합니다.
    """
    data = _lambda_req("POST", "/instance-operations/launch", json={
        "region_name":       region_name,
        "instance_type_name": instance_type,
        "ssh_key_names":     [ssh_key_name],
        "file_system_names": [],
        "quantity":          1,
        "name":              job_name,
    })
    ids = data.get("instance_ids", [])
    if not ids:
        raise RuntimeError(f"Lambda Labs 인스턴스 생성 실패: {data}")
    return ids[0]


def lambda_get_ip(instance_id: str) -> Optional[str]:
    """Lambda Labs 인스턴스의 IP를 반환합니다 (준비 안 됐으면 None)."""
    data = _lambda_req("GET", f"/instances/{instance_id}")
    inst = data.get("data", {})
    if inst.get("status") == "active" and inst.get("ip"):
        return inst["ip"]
    return None


def _lambda_terminate(instance_id: str) -> None:
    """Lambda Labs 인스턴스를 종료합니다."""
    _lambda_req("POST", "/instance-operations/terminate", json={
        "instance_ids": [instance_id]
    })


# ═══════════════════════════════════════════════════════════════════════════════
# IP 폴링 루프
# ═══════════════════════════════════════════════════════════════════════════════

def poll_for_ip(provider: str, instance_id: str) -> str:
    """
    인스턴스가 준비될 때까지 폴링합니다.
    최대 POLL_MAX_WAIT 초 대기 후 타임아웃 에러 발생.
    """
    elapsed  = 0
    dots     = 0
    get_ip   = runpod_get_ip if provider == "RunPod" else lambda_get_ip

    print(f"  {C.GREY}인스턴스 부팅 대기중", end="", flush=True)
    while elapsed < POLL_MAX_WAIT:
        ip = get_ip(instance_id)
        if ip:
            print(f"{C.RESET}")
            return ip

        dots = (dots + 1) % 4
        print(f"\r  {C.GREY}인스턴스 부팅 대기중{'.' * dots}{'  ' * (3 - dots)} ({elapsed}s){C.RESET}", end="", flush=True)
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    print(f"{C.RESET}")
    raise TimeoutError(
        f"{POLL_MAX_WAIT}초 내 인스턴스 준비 완료 안 됨.\n"
        f"    수동 확인: {provider} 대시보드에서 ID={instance_id}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# run 커맨드
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_run(args: argparse.Namespace) -> None:
    gpu_info = _GPU_MAP.get(args.gpu.upper())
    if not gpu_info:
        err(f"지원하지 않는 GPU: {args.gpu}. 사용 가능: {', '.join(_GPU_MAP)}")
        sys.exit(1)

    job_id   = uuid.uuid4().hex[:8]
    job_name = f"gs-{job_id}"

    # ── 헤더 출력 ──────────────────────────────────────────────────────────────
    banner("GridShifter AI  ⚡  Phase 2a  CLI")
    print(f"  {C.BOLD}Job ID   {C.RESET}{C.WHITE}{job_name}{C.RESET}")
    print(f"  {C.BOLD}Image    {C.RESET}{args.image}")
    print(f"  {C.BOLD}GPU      {C.RESET}{gpu_info['display']}  ×{args.count}")
    print(f"  {C.BOLD}Duration {C.RESET}{args.hours}h")
    print(f"  {C.BOLD}Region   {C.RESET}{args.region}")
    if args.dry_run:
        print(f"  {C.YELLOW}[DRY-RUN 모드 — 실제 인스턴스를 생성하지 않습니다]{C.RESET}")

    # ── Step 1: GridShifter 최적 리전 조회 ────────────────────────────────────
    step("1/3  GridShifter 최적 리전 탐색 중…")
    try:
        result  = call_gridshifter(
            gpu_model  = args.gpu.upper(),
            gpu_count  = args.count,
            job_hours  = args.hours,
            region     = args.region,
            workload   = "batch",
        )
    except RuntimeError as exc:
        err(str(exc))
        sys.exit(1)

    rec = result.get("recommended")
    if not rec:
        alert = result.get("workload_alert", {})
        err("추천 리전을 찾지 못했습니다.")
        if alert.get("message"):
            info(f"사유: {alert['message']}")
        sys.exit(1)

    baseline = result.get("baseline_cost_usd", 0)
    savings  = baseline - rec["estimated_total_cost_usd"]
    savings_pct = rec.get("savings_pct", 0)

    ok(f"최적 리전:  {C.CYAN}{C.BOLD}{rec['region']}{C.RESET}  ({rec['provider']})")
    info(f"스팟 가격   : ${rec['spot_price_usd_per_hour']:.3f}/hr")
    info(f"총 예상 비용: ${rec['estimated_total_cost_usd']:.2f}  (절감 ${savings:.2f} / {savings_pct}%)")
    info(f"SSS 점수    : {rec['sss_score']:.4f}")
    info(f"지연(서울)  : {rec.get('latency_ms', 0):.0f}ms")

    # 예상 비용 경고
    hourly_est = rec["spot_price_usd_per_hour"] * args.count
    total_est  = rec["estimated_total_cost_usd"]
    if total_est > 200:
        warn(f"예상 총 비용: ${total_est:.2f} (${hourly_est:.2f}/hr × {args.hours}h × {args.count}대)")
        warn("비용이 $200을 초과합니다. 계속하려면 'yes'를 입력하세요.")
        try:
            ans = input("  확인 (yes/no): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            ans = "no"
        if ans != "yes":
            warn("취소되었습니다.")
            return

    if args.dry_run:
        print(f"\n{C.YELLOW}[DRY-RUN] 여기까지입니다. --dry-run 없이 실행하면 인스턴스를 생성합니다.{C.RESET}")
        return

    # ── Step 2: 인스턴스 생성 ──────────────────────────────────────────────────
    step("2/3  인스턴스 생성 중…")

    provider   = rec["provider"]
    instance_id: str = ""

    # 자동 종료 타이머 경고
    if args.auto_terminate:
        warn(f"--auto-terminate ON: {args.hours}h 후 또는 프로그램 종료 시 인스턴스를 자동 삭제합니다.")

    try:
        if provider == "RunPod":
            try:
                instance_id = runpod_launch(
                    image        = args.image,
                    gpu_type_id  = gpu_info["runpod_id"],
                    gpu_count    = args.count,
                    job_name     = job_name,
                    docker_cmd   = args.cmd or "",
                )
                ok(f"RunPod Pod 생성됨  ID={instance_id}")
            except RuntimeError as runpod_err:
                warn(f"RunPod 실패: {runpod_err}")
                # Lambda Labs로 자동 폴백 시도
                if gpu_info.get("lambda_type") and LAMBDA_API_KEY:
                    warn("Lambda Labs로 자동 폴백 시도합니다…")
                    ssh_keys = lambda_get_ssh_keys()
                    if ssh_keys:
                        ssh_key = args.ssh_key or ssh_keys[0]
                        lambda_region = rec.get("region", "us-tx-3").replace("lambda-", "")
                        instance_id = lambda_launch(
                            instance_type = gpu_info["lambda_type"],
                            region_name   = lambda_region,
                            ssh_key_name  = ssh_key,
                            job_name      = job_name,
                        )
                        provider = "LambdaLabs"
                        ok(f"Lambda Labs 폴백 성공  ID={instance_id}")
                    else:
                        raise RuntimeError("Lambda Labs SSH 키 없음. RunPod 재고도 없습니다.")
                else:
                    raise

        elif provider in ("LambdaLabs", "Lambda Labs"):
            # SSH 키 확인
            ssh_keys = lambda_get_ssh_keys()
            if not ssh_keys:
                err("Lambda Labs 계정에 SSH 키가 등록되지 않았습니다.")
                info("https://cloud.lambdalabs.com/ssh-keys 에서 키를 등록하세요.")
                sys.exit(1)
            ssh_key = args.ssh_key or ssh_keys[0]
            info(f"SSH 키 사용: {ssh_key}")

            # Lambda region ID 파싱 (예: "us-tx-3" → "us-tx-3")
            lambda_region = rec["region"].replace("lambda-", "")
            instance_id   = lambda_launch(
                instance_type = gpu_info["lambda_type"],
                region_name   = lambda_region,
                ssh_key_name  = ssh_key,
                job_name      = job_name,
            )
            ok(f"Lambda Labs 인스턴스 생성됨  ID={instance_id}")

        else:
            # 기타 프로바이더 (RunPod fallback)
            warn(f"'{provider}'는 직접 배포를 지원하지 않습니다. RunPod으로 대체 시도합니다.")
            instance_id = runpod_launch(
                image        = args.image,
                gpu_type_id  = gpu_info["runpod_id"],
                gpu_count    = args.count,
                job_name     = job_name,
                docker_cmd   = args.cmd or "",
            )
            provider = "RunPod"
            ok(f"RunPod Pod 생성됨 (fallback)  ID={instance_id}")

    except RuntimeError as exc:
        err(f"인스턴스 생성 실패: {exc}")
        sys.exit(1)

    # 자동 종료 등록
    _active_instance.update({
        "provider":       provider,
        "id":             instance_id,
        "name":           job_name,
        "auto_terminate": args.auto_terminate,
        "started_at":     time.time(),
        "max_hours":      args.hours,
    })

    # ── Step 3: IP 대기 ────────────────────────────────────────────────────────
    step("3/3  서버 IP 대기 중…")
    try:
        ip = poll_for_ip(provider, instance_id)
    except TimeoutError as exc:
        warn(str(exc))
        ip = None

    # ── 최종 결과 출력 ─────────────────────────────────────────────────────────
    banner("🐳  배포 완료")
    print(f"  {C.BOLD}Provider   {C.RESET}{provider}")
    print(f"  {C.BOLD}Instance   {C.RESET}{instance_id}")
    print(f"  {C.BOLD}Region     {C.RESET}{rec['region']}")
    if ip:
        print(f"  {C.BOLD}IP 주소    {C.RESET}{C.GREEN}{C.BOLD}{ip}{C.RESET}")
        print(f"\n  {C.CYAN}SSH 접속:{C.RESET}  ssh ubuntu@{ip}")
        if args.image:
            print(f"  {C.CYAN}Docker 실행:{C.RESET} docker run --gpus all {args.image}")
    else:
        warn(f"IP를 아직 받지 못했습니다. 대시보드에서 확인하세요.")
        print(f"  Instance ID: {instance_id}")

    print(f"\n  {C.BOLD}예상 비용  {C.RESET}${hourly_est:.2f}/hr → {args.hours}h = ${total_est:.2f}")

    if args.auto_terminate:
        print(f"\n  {C.YELLOW}⏱  {args.hours}h 후 자동 종료 예약됨{C.RESET}")
        # 별도 스레드에서 타이머 실행
        import threading
        def _auto_terminate_timer():
            time.sleep(args.hours * 3600)
            warn(f"작업 시간 {args.hours}h 경과 — 인스턴스 자동 종료 중…")
            try:
                if provider == "RunPod":
                    _runpod_terminate(instance_id)
                else:
                    _lambda_terminate(instance_id)
                ok("자동 종료 완료")
            except Exception as e:
                err(f"자동 종료 실패: {e}")
        t = threading.Thread(target=_auto_terminate_timer, daemon=True)
        t.start()
        _active_instance["auto_terminate"] = False  # atexit 중복 방지 (스레드가 처리)

    print()

    # 결과 JSON 저장 (선택)
    if args.save:
        out = {
            "job_id":      job_name,
            "provider":    provider,
            "instance_id": instance_id,
            "region":      rec["region"],
            "ip":          ip,
            "gpu":         gpu_info["display"],
            "count":       args.count,
            "hours":       args.hours,
            "image":       args.image,
            "cost_usd":    total_est,
            "savings_usd": round(savings, 2),
            "created_at":  datetime.now(timezone.utc).isoformat(),
        }
        out_path = Path(f"gs-job-{job_name}.json")
        out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False))
        ok(f"결과 저장: {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# status 커맨드
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_status(args: argparse.Namespace) -> None:
    """GridShifter 백엔드 상태 확인."""
    try:
        resp = requests.get(f"{GRIDSHIFTER_API}/api/v1/health", timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        err(f"백엔드 응답 없음: {exc}")
        sys.exit(1)

    banner("GridShifter 백엔드 상태")
    status = data.get("status", "unknown")
    if status == "ok":
        ok(f"API 서버 정상  (업타임 {data.get('uptime_seconds', 0):.0f}s)")
    else:
        warn(f"상태: {status}")

    info(f"API:     {GRIDSHIFTER_API}")
    info(f"RunPod:  {'✓ 키 있음' if RUNPOD_API_KEY else '✗ RUNPOD_API_KEY 없음'}")
    info(f"Lambda:  {'✓ 키 있음' if LAMBDA_API_KEY else '✗ LAMBDA_LABS_API_KEY 없음'}")
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# 메인 엔트리포인트
# ═══════════════════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gridshifter",
        description="⚡ GridShifter AI — 글로벌 GPU 최적 배포 CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python gridshifter.py run --image pytorch/pytorch --gpu H100 --count 4 --hours 8
  python gridshifter.py run --image nvcr.io/nvidia/cuda:12-devel --gpu A100 --dry-run
  python gridshifter.py run --image myrepo/train:latest --gpu H100 --auto-terminate --save
  python gridshifter.py status
        """,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── run ──
    run_p = sub.add_parser("run", help="최적 리전을 찾아 인스턴스를 생성합니다")
    run_p.add_argument("--image",   required=True,  help="Docker 이미지 (예: pytorch/pytorch:latest)")
    run_p.add_argument("--gpu",     default="L40",   help="GPU 모델: H100 / H200 / A100 / L40 / A10  [기본: L40]")
    run_p.add_argument("--count",   type=int, default=1, help="GPU 수량  [기본: 1]")
    run_p.add_argument("--hours",   type=float, default=8.0, help="작업 시간 (시간)  [기본: 8]")
    run_p.add_argument("--region",  default="ap-northeast-2", help="베이스라인 리전  [기본: ap-northeast-2]")
    run_p.add_argument("--cmd",     default="",     help="컨테이너 시작 명령어 (RunPod dockerArgs)")
    run_p.add_argument("--ssh-key", dest="ssh_key", default=None, help="Lambda Labs SSH 키 이름 (없으면 첫 번째 키 사용)")
    run_p.add_argument("--dry-run", action="store_true", help="실제 배포 없이 최적 리전만 조회")
    run_p.add_argument("--auto-terminate", action="store_true", help="--hours 경과 후 인스턴스 자동 종료")
    run_p.add_argument("--save",    action="store_true", help="결과를 JSON 파일로 저장")

    # ── status ──
    sub.add_parser("status", help="GridShifter 백엔드 연결 상태 확인")

    return parser


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "status":
        cmd_status(args)


if __name__ == "__main__":
    main()
