#!/usr/bin/env python3
"""把 java-native-api-testcase 的用例导入 TimechoPLM，并生成 PLM 编号映射。

用法（需要 PAT，令牌只从环境变量读取）：
  export TIMECHO_PLM_BASE_URL=https://plm.infra.timecho.com
  export TIMECHO_PLM_TOKEN=plm_pat_xxx
  python3 scripts/plm/import_to_plm.py --product-id 2 --feature-id 123

可选：
  --dry-run        只打印将导入的用例，不发请求、不写映射
  --limit N        最多导入 N 条（分批验证用）
  --owner NAME     负责人（默认不传）
  --case-type      用例类型（默认「功能测试」）
  --model          模型（默认「通用」）
  --map            映射文件路径（默认 .plm-case-id-map.json）

映射格式：{ "FQCN::method": "TimechoDB-123" }（程序自然 ID → PLM 公开编号）。
导入成功后 list-cases 输出 PLM 编号，case <PLM编号> 按映射反查自然 ID 执行。
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from plm_cases import CASE_MAP, repo_root, scan_cases

DEFAULT_AUTOMATION_TYPE = "session-java"


def log(message: str) -> None:
    sys.stderr.write(f"[import] {message}\n")


def load_map(path: Path) -> dict:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"[import] {path} 必须是 JSON 对象")
    return {str(k): str(v) for k, v in data.items() if str(v).strip()}


def write_map(path: Path, mapping: dict) -> None:
    ordered = {key: mapping[key] for key in sorted(mapping, key=str.lower)}
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(ordered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
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
    url = base_url.rstrip("/") + "/api/open/test-cases"
    last_error = None
    for attempt in range(3):
        try:
            result = request_json("POST", url, token, payload)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            last_error = f"HTTP {exc.code}: {detail}"
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            last_error = str(exc)
        else:
            if result.get("code") != 200:
                raise RuntimeError(result.get("message") or "PLM 返回业务错误")
            data = result.get("data")
            if not isinstance(data, dict):
                raise RuntimeError("PLM 响应缺少 data")
            return data
        if attempt < 2:
            time.sleep(1 + attempt)
    raise RuntimeError(last_error or "请求失败")


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
    mapping = load_map(map_path)
    items = scan_cases(repo_root(), DEFAULT_AUTOMATION_TYPE)
    pending = [item for item in items if item["_rawCaseId"] not in mapping]
    if args.limit > 0:
        pending = pending[: args.limit]

    log(f"用例总数 {len(items)}，已映射 {len(mapping)}，本次待导入 {len(pending)}")
    if args.dry_run:
        for item in pending[:10]:
            log(f"DRY {item['_rawCaseId']} -> {item['sourceFile']}")
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
            failed.append((item["_rawCaseId"], str(exc)))
            log(f"[{index}/{len(pending)}] 失败 {item['_rawCaseId']}: {exc}")
            continue
        plm_id = data.get("externalCaseId")
        if not plm_id:
            failed.append((item["_rawCaseId"], "响应缺少 externalCaseId"))
            log(f"[{index}/{len(pending)}] 失败 {item['_rawCaseId']}: 响应缺少 externalCaseId")
            continue
        mapping[item["_rawCaseId"]] = plm_id
        created += 1
        if index == 1 or index % 20 == 0 or index == len(pending):
            log(f"[{index}/{len(pending)}] {item['_rawCaseId']} -> {plm_id}")
        time.sleep(0.05)

    if mapping:
        write_map(map_path, mapping)
        log(f"映射已写入 {map_path}（{len(mapping)} 条）")
    log(f"完成：新建 {created}，失败 {len(failed)}")
    for raw_id, reason in failed[:20]:
        log(f"  FAILED {raw_id}: {reason}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
