#!/usr/bin/env python3
"""PLM 适配工具：扫描 TestNG 用例、查询映射、生成单用例 TestNG XML。

只读仓库源码，不写任何映射缓存，保证执行前后 git 工作区洁净。
stdout 只输出 JSON（list 子命令），诊断一律写 stderr。
"""

import hashlib
import json
import os
import re
import sys
from pathlib import Path
from xml.sax.saxutils import escape

MAX_CASE_ID = 100
DEFAULT_AUTOMATION_TYPE = "session-java"

TEST_SOURCE_DIR = Path("details/src/test/java")
TESTNG_XML = Path("details/src/main/resources/testng.xml")
CASE_MAP = Path(".plm-case-id-map.json")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def strip_comments(text: str) -> str:
    """去掉 Java 行注释与块注释，保留字符串/字符字面量内容。"""
    out = []
    i = 0
    n = len(text)
    state = "normal"
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if state == "normal":
            if ch == "/" and nxt == "/":
                state = "line_comment"
                i += 2
                continue
            if ch == "/" and nxt == "*":
                state = "block_comment"
                i += 2
                continue
            if ch == '"':
                state = "string"
                out.append(ch)
                i += 1
                continue
            if ch == "'":
                state = "char"
                out.append(ch)
                i += 1
                continue
            out.append(ch)
            i += 1
        elif state == "line_comment":
            if ch == "\n":
                state = "normal"
                out.append(ch)
            i += 1
        elif state == "block_comment":
            if ch == "*" and nxt == "/":
                state = "normal"
                i += 2
            else:
                if ch == "\n":
                    out.append(ch)
                i += 1
        elif state == "string":
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if ch == '"':
                state = "normal"
            i += 1
        else:  # char
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if ch == "'":
                state = "normal"
            i += 1
    return "".join(out)


def enabled_classes(root: Path) -> set:
    """testng.xml 中启用的测试类；文件缺失时返回空集合（表示不限制）。"""
    path = root / TESTNG_XML
    if not path.is_file():
        return set()
    text = read_text(path)
    return set(re.findall(r'<class\s+name="([^"]+)"', text))


def bounded_case_id(raw: str) -> str:
    if len(raw) <= MAX_CASE_ID:
        return raw
    suffix = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return raw[: MAX_CASE_ID - 17] + "." + suffix


def load_case_map(root: Path) -> dict:
    """读取 PLM 编号映射（扁平 JSON：程序自然 ID -> PLM 公开编号）。

    文件由 import_to_plm.py 在导入后生成；不存在时返回空映射，此时
    list-cases 输出自然 ID，兼容尚未导入 PLM 的使用方式。
    """
    path = root / CASE_MAP
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"[plm] 读取 {CASE_MAP} 失败: {exc}")
    if not isinstance(data, dict):
        raise SystemExit(f"[plm] {CASE_MAP} 必须是 JSON 对象")
    mapping = {}
    for raw_id, plm_id in data.items():
        if not isinstance(plm_id, str) or not plm_id.strip():
            continue
        mapping[str(raw_id).strip().upper()] = plm_id.strip()
    return mapping


def scan_cases(root: Path, automation_type: str) -> list:
    test_dir = root / TEST_SOURCE_DIR
    if not test_dir.is_dir():
        raise SystemExit(f"[plm] 测试源码目录不存在: {test_dir}")
    enabled = enabled_classes(root)
    by_id = {}
    for path in sorted(test_dir.rglob("*.java")):
        text = strip_comments(read_text(path))
        pkg_match = re.search(r"^\s*package\s+([\w.]+)\s*;", text, re.MULTILINE)
        fqcn = (pkg_match.group(1) + "." if pkg_match else "") + path.stem
        if enabled and fqcn not in enabled:
            continue
        if re.search(r"\babstract\s+class\b", text):
            continue
        for match in re.finditer(r"@Test\b", text):
            tail = text[match.end():].split("@Test", 1)[0]
            decl = re.search(
                r"\bpublic\s+(?:static\s+|final\s+|synchronized\s+)*"
                r"[\w.<>\[\], ?]+\s+(\w+)\s*\(",
                tail,
            )
            if not decl:
                continue
            method = decl.group(1)
            if method == path.stem:
                continue
            raw_id = f"{fqcn}::{method}"
            case_id = bounded_case_id(raw_id)
            key = case_id.upper()
            if key in by_id:
                raise SystemExit(
                    f"[plm] CASE_ID 冲突（大小写归一后重复）: {case_id}"
                )
            by_id[key] = {
                "caseId": case_id,
                "_rawCaseId": case_id,
                "automationType": automation_type,
                "nodeId": raw_id,
                "sourceFile": str(path.relative_to(root)).replace(os.sep, "/"),
                "description": "",
                "_fqcn": fqcn,
                "_method": method,
            }
    case_map = load_case_map(root)
    if case_map:
        remapped = {}
        for item in by_id.values():
            mapped = case_map.get(item["_rawCaseId"].upper())
            if mapped:
                item["caseId"] = mapped
            key = item["caseId"].upper()
            if key in remapped:
                raise SystemExit(f"[plm] 映射后 CASE_ID 冲突（大小写归一后重复）: {item['caseId']}")
            remapped[key] = item
        return sorted(remapped.values(), key=lambda item: item["caseId"].lower())
    return sorted(by_id.values(), key=lambda item: item["caseId"].lower())


def public_item(item: dict) -> dict:
    return {
        "caseId": item["caseId"],
        "automationType": item["automationType"],
        "nodeId": item["nodeId"],
        "sourceFile": item["sourceFile"],
        "description": item["description"],
    }


def cmd_list(root: Path, automation_type: str) -> int:
    items = scan_cases(root, automation_type)
    json.dump([public_item(item) for item in items], sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


def find_case(root: Path, case_id: str) -> dict:
    normalized = case_id.strip().upper()
    items = scan_cases(root, DEFAULT_AUTOMATION_TYPE)
    for item in items:
        if item["caseId"].upper() == normalized:
            return item
    # 兼容尚未生成映射时直接使用程序自然 ID 的调用方式。
    for item in items:
        if item["_rawCaseId"].upper() == normalized:
            return item
    raise SystemExit(f"[plm] CASE_ID 未找到: {case_id}")


def cmd_lookup(root: Path, case_id: str) -> int:
    item = find_case(root, case_id)
    sys.stdout.write(f"{item['_fqcn']}\t{item['_method']}\n")
    return 0


def cmd_testng_xml(root: Path, case_id: str, output: str) -> int:
    item = find_case(root, case_id)
    content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<suite name="plm-single" verbose="1" parallel="false">\n'
        '  <test name="plm-single">\n'
        "    <classes>\n"
        f'      <class name="{escape(item["_fqcn"])}">\n'
        "        <methods>\n"
        f'          <include name="{escape(item["_method"])}"/>\n'
        "        </methods>\n"
        "      </class>\n"
        "    </classes>\n"
        "  </test>\n"
        "</suite>\n"
    )
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8", newline="\n")
    return 0


def main(argv: list) -> int:
    configure_stdout()
    root = repo_root()
    if len(argv) < 2:
        sys.stderr.write(
            "Usage: plm_cases.py list [--automation-type T] | lookup <caseId> | "
            "testng-xml <caseId> <output>\n"
        )
        return 2
    command = argv[1]
    if command == "list":
        automation_type = DEFAULT_AUTOMATION_TYPE
        if "--automation-type" in argv:
            automation_type = argv[argv.index("--automation-type") + 1]
        return cmd_list(root, automation_type)
    if command == "lookup" and len(argv) >= 3:
        return cmd_lookup(root, argv[2])
    if command == "testng-xml" and len(argv) >= 4:
        return cmd_testng_xml(root, argv[2], argv[3])
    sys.stderr.write("Usage: plm_cases.py list [--automation-type T] | lookup <caseId> | "
                     "testng-xml <caseId> <output>\n")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
