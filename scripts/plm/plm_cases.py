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
    """testng.xml 中启用的测试类；文件缺失时返回空集合（表示不限制）。

    先剥离 XML 注释：被注释掉的 <class> 是项目刻意关闭的套件（如 TestSQL/Trigger/UDF），
    不能进入清单，否则 PLM 会派发并真正执行它们。
    """
    path = root / TESTNG_XML
    if not path.is_file():
        return set()
    text = re.sub(r"<!--.*?-->", "", read_text(path), flags=re.DOTALL)
    return set(re.findall(r'<class\s+name="([^"]+)"', text))


def _load_map_data(root: Path) -> dict:
    path = root / CASE_MAP
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"[plm] 读取 {CASE_MAP} 失败: {exc}")
    if not isinstance(data, dict):
        raise SystemExit(f"[plm] {CASE_MAP} 必须是 JSON 对象")
    return data


def bounded_case_id(raw: str) -> str:
    if len(raw) <= MAX_CASE_ID:
        return raw
    suffix = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return raw[: MAX_CASE_ID - 17] + "." + suffix


def load_case_map(root: Path) -> dict:
    """读取 PLM 编号映射，返回 target(大写) -> 当前 PLM 编号。

    兼容两种格式：
    - 扁平 JSON（手工/旧格式）：{程序自然 ID: PLM 公开编号}
    - 导入 skill 生成的 {nmap: {PLM编号: [target...]}, revmap: {target: PLM编号}, aliases}
    """
    data = _load_map_data(root)
    mapping = {}

    # skill 格式：revmap 直接是自然 ID -> PLM 编号
    revmap = data.get("revmap")
    if isinstance(revmap, dict):
        for raw_id, plm_id in revmap.items():
            if isinstance(plm_id, str) and plm_id.strip():
                mapping[str(raw_id).strip().upper()] = plm_id.strip()

    # skill 格式：nmap 是 PLM 编号 -> 自然 ID 列表；revmap 缺失时由它反转
    nmap = data.get("nmap")
    if isinstance(nmap, dict):
        for plm_id, targets in nmap.items():
            if not isinstance(plm_id, str) or not plm_id.strip():
                continue
            if isinstance(targets, str):
                targets = [targets]
            if not isinstance(targets, list):
                continue
            for target in targets:
                if not isinstance(target, str) or not target.strip():
                    continue
                mapping.setdefault(target.strip().upper(), plm_id.strip())

    # 扁平格式：{自然 ID: PLM 编号}（仅在没有 nmap/revmap 时使用，避免把元数据键当映射）
    if mapping:
        return mapping
    for raw_id, plm_id in data.items():
        if isinstance(plm_id, str) and plm_id.strip():
            mapping[str(raw_id).strip().upper()] = plm_id.strip()
    return mapping


def load_alias_map(root: Path) -> dict:
    """aliases 段（旧 PLM 编号 -> target），用于恢复 remap 后仍下发旧编号的场景。

    与 nmap 同构；不并入 load_case_map，避免破坏 1:1 与同步检查语义。
    """
    data = _load_map_data(root)
    aliases = data.get("aliases")
    result = {}
    if not isinstance(aliases, dict):
        return result
    for old_id, targets in aliases.items():
        if not isinstance(old_id, str) or not old_id.strip():
            continue
        if isinstance(targets, str):
            targets = [targets]
        if not isinstance(targets, list):
            continue
        for target in targets:
            if isinstance(target, str) and target.strip():
                result[old_id.strip().upper()] = target.strip()
    return result


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
            # @Test(enabled = false) 的方法不会被 TestNG 执行：不登记、不导入。
            if re.search(r"\benabled\s*=\s*false\b", tail[: decl.start()], re.IGNORECASE):
                continue
            method = decl.group(1)
            if method == path.stem:
                continue
            raw_id = f"{fqcn}::{method}"
            case_id = bounded_case_id(raw_id)
            key = raw_id.upper()
            if key in by_id:
                raise SystemExit(
                    f"[plm] 测试目标重复（大小写归一后重复）: {raw_id}"
                )
            by_id[key] = {
                "caseId": case_id,
                "_rawCaseId": case_id,
                # 完整 target：映射/导入/pending 判断必须用完整值；
                # _rawCaseId 只是超过 100 字符时的对外回退编号。
                "_target": raw_id,
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
            mapped = case_map.get(item["_target"].upper())
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
    # 兼容尚未生成映射时直接使用程序自然 ID 的调用方式（完整 target 优先）。
    for item in items:
        if item["_target"].upper() == normalized:
            return item
    for item in items:
        if item["_rawCaseId"].upper() == normalized:
            return item
    # 恢复 remap 后 PLM 可能仍下发旧编号：按 aliases 找到 target。
    alias_target = load_alias_map(root).get(normalized)
    if alias_target:
        for item in items:
            if item["_target"].upper() == alias_target.upper():
                return item
    raise SystemExit(f"[plm] CASE_ID 未找到: {case_id}")


def cmd_lookup(root: Path, case_id: str) -> int:
    item = find_case(root, case_id)
    sys.stdout.write(f"{item['_fqcn']}\t{item['_method']}\n")
    return 0


def class_methods(root: Path, fqcn: str):
    """从 testng.xml 读取该类登记的全部方法，保持原始顺序。

    这些用例类大量依赖同类的 @BeforeClass / 前置方法（例如先 createTemplate
    再 insert），单独跑一个方法会制造大量假失败。PLM 每条用例仍只回填目标
    方法的 case_id，但执行时按原测试套件顺序跑完整类，保证前置状态成立。
    """
    path = root / TESTNG_XML
    if not path.is_file():
        return []
    text = read_text(path)
    pattern = re.compile(
        r'<class\s+name="' + re.escape(fqcn) + r'"\s*>(.*?)</class>',
        re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        return []
    return re.findall(r'<include\s+name="([^"]+)"\s*/>', match.group(1))


def cmd_testng_xml(root: Path, case_id: str, output: str) -> int:
    item = find_case(root, case_id)
    methods = class_methods(root, item["_fqcn"])
    if not methods:
        methods = [item["_method"]]
    includes = "".join(
        f'          <include name="{escape(method)}"/>\n' for method in methods
    )
    content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<suite name="plm-single" verbose="1" parallel="false">\n'
        '  <test name="plm-single">\n'
        "    <classes>\n"
        f'      <class name="{escape(item["_fqcn"])}">\n'
        "        <methods>\n"
        f"{includes}"
        "        </methods>\n"
        "      </class>\n"
        "    </classes>\n"
        "  </test>\n"
        "</suite>\n"
    )
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    # 不用 Path.write_text(newline=...)：该参数 Python 3.10+ 才支持，执行机可能是 3.8。
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)
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
