#!/usr/bin/env python3
"""把 java-native-api-testcase 的用例导入 TimechoPLM，并回填 PLM 编号映射。

用法（需要 PAT，令牌只从环境变量读取）：
  export TIMECHO_PLM_BASE_URL=https://plm.infra.timecho.com
  export TIMECHO_PLM_TOKEN=plm_pat_xxx
  python3 scripts/plm/import_to_plm.py --product-id 1 --feature-id 2023

可选：
  --dry-run          只打印将导入的用例，不发请求、不写映射
  --limit N          最多导入 N 条（分批验证用）
  --owner NAME       负责人（默认不传）
  --case-type        用例类型（默认「功能测试」）
  --model            模型（默认「通用」）
  --automation-type  自动化类型（默认 session-java）
  --map              映射文件路径（默认 .plm-case-id-map.json）

映射兼容扁平与导入 skill 的 {nmap, revmap, aliases} 格式；写入时保留原格式、
aliases 与未知顶层键。每条创建成功后立即原子落盘，避免中断后重复创建。
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from plm_cases import CASE_MAP, load_case_map, repo_root, scan_cases

DEFAULT_AUTOMATION_TYPE = "session-java"


def log(message: str) -> None:
    sys.stderr.write(f"[import] {message}\n")


def load_raw_map(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"[import] 读取 {path} 失败: {exc}")
    if not isinstance(data, dict):
        raise SystemExit(f"[import] {path} 必须是 JSON 对象")
    return data


def write_map(path: Path, raw: dict, mapping: dict, originals: dict) -> None:
    """原子写回映射；技能格式保留 aliases 与未知顶层键。"""
    if isinstance(raw.get("nmap"), dict) or isinstance(raw.get("revmap"), dict):
        nmap = raw.setdefault("nmap", {})
        revmap = raw.setdefault("revmap", {})
        for upper, plm_id in mapping.items():
            target = originals.get(upper, upper)
            revmap[target] = plm_id
            targets = nmap.setdefault(plm_id, [])
            if isinstance(targets, str):
                targets = [targets]
                nmap[plm_id] = targets
            if target not in targets:
                targets.append(target)
        data = raw
    else:
        data = {
            originals.get(upper, upper): mapping[upper]
            for upper in sorted(mapping, key=lambda key: originals.get(key, key).lower())
        }

    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def request_json(method: str, url: str, token: str, payload=None):
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url, data=body, method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def create_case(base_url: str, token: str, payload: dict) -> dict:
    """单次 POST。不自动重试：服务端可能已创建成功但响应超时，重试会产生重复用例。"""
    url = base_url.rstrip("/") + "/api/open/test-cases"
    try:
        result = request_json("POST", url, token, payload)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"HTTP {exc.code}: {detail}")
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        raise RuntimeError(str(exc))
    if result.get("code") != 200:
        raise RuntimeError(result.get("message") or "PLM 返回业务错误")
    data = result.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("PLM 响应缺少 data")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product-id", type=int, required=True)
    parser.add_argument("--feature-id", type=int, action="append", required=True,
                        help="关联功能点 ID，可重复")
    parser.add_argument("--owner", default=None)
    parser.add_argument("--case-type", default="功能测试")
    parser.add_argument("--model", default="通用")
    parser.add_argument("--automation-type", default=DEFAULT_AUTOMATION_TYPE)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--map", default=str(repo_root() / CASE_MAP))
    args = parser.parse_args()

    base_url = os.environ.get("TIMECHO_PLM_BASE_URL", "").strip()
    token = os.environ.get("TIMECHO_PLM_TOKEN", "").strip()
    if not args.dry_run and (not base_url or not token):
        raise SystemExit("[import] 需要 TIMECHO_PLM_BASE_URL 与 TIMECHO_PLM_TOKEN")

    map_path = Path(args.map)
    raw = load_raw_map(map_path)
    mapping = load_case_map(map_path)
    originals = {}
    items = scan_cases(repo_root(), DEFAULT_AUTOMATION_TYPE)
    for item in items:
        originals[item["_target"].upper()] = item["_target"]
    pending = [item for item in items if item["_target"].upper() not in mapping]
    if args.limit > 0:
        pending = pending[: args.limit]

    log(f"用例总数 {len(items)}，已映射 {len(mapping)}，本次待导入 {len(pending)}")
    if args.dry_run:
        for item in pending[:10]:
            log(f"DRY {item['_target']} -> {item['sourceFile']}")
        if len(pending) > 10:
            log(f"DRY ... 其余 {len(pending) - 10} 条省略")
        return 0

    created = 0
    failed = []
    for index, item in enumerate(pending, start=1):
        payload = {
            "name": item["_method"],
            "caseType": args.case_type,
            "executeMethod": "自动",
            "automationType": args.automation_type,
            "model": args.model,
            "productId": args.product_id,
            "featureIds": args.feature_id,
            "description": item["nodeId"],
        }
        if args.owner:
            payload["owner"] = args.owner
        try:
            data = create_case(base_url, token, payload)
        except RuntimeError as exc:
            failed.append((item["_target"], str(exc)))
            log(f"[{index}/{len(pending)}] 失败 {item['_target']}: {exc}")
            continue
        plm_id = data.get("externalCaseId")
        if not plm_id:
            failed.append((item["_target"], "响应缺少 externalCaseId"))
            log(f"[{index}/{len(pending)}] 失败 {item['_target']}: 响应缺少 externalCaseId")
            continue
        mapping[item["_target"].upper()] = plm_id
        originals[item["_target"].upper()] = item["_target"]
        # 每条成功后立即落盘：进程中断后重跑不会重复创建已成功的用例。
        write_map(map_path, raw, mapping, originals)
        created += 1
        if index == 1 or index % 20 == 0 or index == len(pending):
            log(f"[{index}/{len(pending)}] {item['_target']} -> {plm_id}")
        time.sleep(0.05)

    if created:
        log(f"映射已写入 {map_path}（{len(mapping)} 条）")
    log(f"完成：新建 {created}，失败 {len(failed)}")
    for raw_id, reason in failed[:20]:
        log(f"  FAILED {raw_id}: {reason}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
