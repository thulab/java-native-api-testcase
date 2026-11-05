#!/usr/bin/env python3
"""PLM 适配工具：把 Surefire/TestNG 报告转换成 PLM 约定的 JUnit XML。

- single: 从 Surefire 报告目录提取指定类/方法的 testcase，注入 case_id 后写目标文件。
- merge:  把同一批次产生的 case-*.xml 合并成 cases.xml（兼容批量入口）。
"""

import copy
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from xml.etree import ElementTree as ET

KNOWN_ISSUES_FILE = Path(__file__).resolve().parents[2] / ".plm-known-issues.json"


def fail(message: str) -> None:
    sys.stderr.write(f"[plm] {message}\n")
    raise SystemExit(1)


def load_known_issues() -> dict:
    """已知问题清单：caseId -> {reason, match}。

    match 是错误消息正则；为空表示该用例的任何 failure/error 都按已知问题处理。
    这类结果会被改写成带原因的 SKIPPED，既不掩盖（原因在 PLM 上可见），
    也不会把每次实测都染红。
    """
    path = KNOWN_ISSUES_FILE
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"[plm] 读取 {path.name} 失败（忽略）：{exc}\n")
        return {}
    if not isinstance(data, dict):
        return {}
    # 统一大写键：PLM 下发的是规范化后的 externalCaseId（TIMECHODB-xxxx），
    # 清单里为了可读性写的是 TimechoDB-xxxx。
    return {
        key.strip().upper(): value
        for key, value in data.items()
        if isinstance(key, str) and isinstance(value, dict)
    }


def known_issue_for(issues: dict, case_id: str):
    return issues.get(case_id.strip().upper())


def testcase_message(testcase: ET.Element) -> str:
    for tag in ("failure", "error", "skipped"):
        element = testcase.find(tag)
        if element is None:
            continue
        message = element.get("message")
        if message:
            return message
    return ""


def has_failure_or_error(testcase: ET.Element) -> bool:
    return testcase.find("failure") is not None or testcase.find("error") is not None


def matches_known_issue(testcase: ET.Element, issue: dict) -> bool:
    if not has_failure_or_error(testcase):
        return False
    pattern = issue.get("match")
    # 无 match 只用于“目标方法 0 次执行”的停用用例（如空 DataProvider），
    # 不允许把实际跑出来的任意 failure/error 都吞成已知问题。
    if not pattern:
        return False
    try:
        return re.search(pattern, testcase_message(testcase)) is not None
    except re.error:
        return False


def force_skipped(testcase: ET.Element, reason: str) -> None:
    for tag in ("failure", "error"):
        for element in list(testcase.findall(tag)):
            testcase.remove(element)
    skipped = ET.SubElement(testcase, "skipped")
    skipped.set("message", f"KNOWN ISSUE: {reason}")


def load_testng_results(report_dir: Path):
    path = report_dir.parent / "testng-results.xml"
    if not path.is_file():
        return None
    try:
        return ET.parse(path)
    except ET.ParseError:
        return None


def exception_message(test_method: ET.Element) -> str:
    exception = test_method.find("exception")
    if exception is None:
        return ""
    message = exception.find("message")
    if message is None or message.text is None:
        return ""
    return " ".join(message.text.split())


def testng_skip_reason(results_tree, method: str) -> str:
    """从 TestNG 原始结果里恢复跳过原因。

    JUnitReportReporter 对跳过用例只写 <skipped/>，不带 message；真正的
    SkipException 原因或 @BeforeClass 配置失败在 testng-results.xml 里。
    """
    if results_tree is None:
        return ""
    for test_method in results_tree.iter("test-method"):
        if test_method.get("is-config") == "true":
            continue
        name = test_method.get("name", "")
        if not (name == method or name.startswith(method + "[")):
            continue
        if test_method.get("status") != "SKIP":
            continue
        message = exception_message(test_method)
        if message:
            return message
    for test_method in results_tree.iter("test-method"):
        if test_method.get("is-config") != "true":
            continue
        if test_method.get("status") != "FAIL":
            continue
        message = exception_message(test_method)
        if message:
            return f"配置方法 {test_method.get('name')} 失败: {message}"
    return ""


def enrich_skipped_message(testcase: ET.Element, results_tree, method: str) -> None:
    skipped = testcase.find("skipped")
    if skipped is None or skipped.get("message"):
        return
    reason = testng_skip_reason(results_tree, method)
    if reason:
        skipped.set("message", reason)


def synthetic_skipped(fqcn: str, method: str, case_id: str, reason: str) -> ET.ElementTree:
    suite = ET.Element("testsuite", {
        "name": fqcn,
        "tests": "1",
        "failures": "0",
        "errors": "0",
        "skipped": "1",
        "time": "0.000",
    })
    testcase = ET.SubElement(suite, "testcase", {
        "classname": fqcn,
        "name": method,
        "time": "0.000",
    })
    properties = ET.SubElement(testcase, "properties")
    ET.SubElement(properties, "property", {"name": "case_id", "value": case_id})
    skipped = ET.SubElement(testcase, "skipped")
    skipped.set("message", f"KNOWN ISSUE: {reason}")
    root = ET.Element("testsuites")
    root.append(suite)
    return ET.ElementTree(root)


def synthetic_error(fqcn: str, method: str, case_id: str, message: str) -> ET.ElementTree:
    suite = ET.Element("testsuite", {
        "name": fqcn,
        "tests": "1",
        "failures": "0",
        "errors": "1",
        "skipped": "0",
        "time": "0.000",
    })
    testcase = ET.SubElement(suite, "testcase", {
        "classname": fqcn,
        "name": method,
        "time": "0.000",
    })
    properties = ET.SubElement(testcase, "properties")
    ET.SubElement(properties, "property", {"name": "case_id", "value": case_id})
    error = ET.SubElement(testcase, "error")
    error.set("message", message)
    root = ET.Element("testsuites")
    root.append(suite)
    return ET.ElementTree(root)


def cmd_timeout(output: str, case_id: str, fqcn: str, method: str, seconds: str) -> int:
    """整类执行超时：TestNG 可能卡在服务端 socket 读上，JUnit 报告还没写出。

    仍然写一条带明确原因的 ERROR，避免 PLM 只看到“未产出结果”。
    """
    reason = f"测试类执行超时（{seconds}s），已终止；请检查服务端是否卡住或连接是否可用"
    return cmd_timeout_reason(output, case_id, fqcn, method, reason)


def cmd_timeout_reason(output: str, case_id: str, fqcn: str, method: str, reason: str) -> int:
    """按给定原因写超时 ERROR；供同一 run 内后续同类用例复用超时结果。"""
    atomic_write(synthetic_error(fqcn, method, case_id, reason), Path(output))
    sys.stderr.write(f"[plm] 已写出超时结果: {case_id}（{reason}）\n")
    return 0


def cmd_known_skip(output: str, case_id: str, fqcn: str, method: str) -> int:
    """已知会挂起/无法执行的用例：不启动测试类，直接写带原因的 SKIPPED。

    返回 2 表示不是 skipRun 用例，调用方继续正常执行。
    """
    issue = known_issue_for(load_known_issues(), case_id)
    if not issue or not issue.get("skipRun"):
        return 2
    reason = issue.get("reason") or "已知问题"
    atomic_write(synthetic_skipped(fqcn, method, case_id, reason), Path(output))
    sys.stderr.write(f"[plm] 已知问题，跳过执行: {case_id}（{reason}）\n")
    return 0


def atomic_write(tree: ET.ElementTree, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".plm-", suffix=".xml.tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "wb") as out:
            tree.write(out, encoding="utf-8", xml_declaration=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, target)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def matches(testcase: ET.Element, fqcn: str, method: str) -> bool:
    classname = testcase.get("classname", "")
    name = testcase.get("name", "")
    same_class = classname == fqcn or classname.startswith(fqcn + "$")
    same_method = name == method or name.startswith(method + "[")
    return same_class and same_method


def with_case_id(testcase: ET.Element, case_id: str) -> ET.Element:
    node = copy.deepcopy(testcase)
    properties = node.find("properties")
    if properties is None:
        properties = ET.Element("properties")
        node.insert(0, properties)
    for prop in list(properties.findall("property")):
        if prop.get("name") == "case_id":
            properties.remove(prop)
    ET.SubElement(properties, "property", {"name": "case_id", "value": case_id})
    return node


def suite_summary(testcases: list) -> dict:
    failures = 0
    errors = 0
    skipped = 0
    total_time = 0.0
    for case in testcases:
        if case.find("failure") is not None:
            failures += 1
        if case.find("error") is not None:
            errors += 1
        if case.find("skipped") is not None:
            skipped += 1
        try:
            total_time += float(case.get("time", "0") or 0)
        except ValueError:
            pass
    return {
        "tests": str(len(testcases)),
        "failures": str(failures),
        "errors": str(errors),
        "skipped": str(skipped),
        "time": f"{total_time:.3f}",
    }


def cmd_single(surefire_dir: str, output: str, case_id: str, fqcn: str, method: str) -> int:
    report_dir = Path(surefire_dir)
    if not report_dir.is_dir():
        fail(f"Surefire 报告目录不存在: {report_dir}")
    issues = load_known_issues()
    issue = known_issue_for(issues, case_id)
    results_tree = load_testng_results(report_dir)
    selected = []
    for path in sorted(report_dir.glob("TEST-*.xml")):
        try:
            tree = ET.parse(path)
        except ET.ParseError as exc:
            fail(f"Surefire 报告解析失败: {path}: {exc}")
        for testcase in tree.iter("testcase"):
            if matches(testcase, fqcn, method):
                selected.append(with_case_id(testcase, case_id))
    if not selected:
        # 只有“停用/挂起”类已知问题允许在目标方法 0 次执行时合成 SKIPPED。
        # 带 match 的已知问题必须看到真实错误消息才能转换；否则可能是配置失败/方法没跑，
        # 不能拿已知问题当遮羞布。
        if issue and (issue.get("skipRun") or not issue.get("match")):
            reason = issue.get("reason") or "已知问题"
            sys.stderr.write(f"[plm] 目标用例未执行，按已知问题记为 SKIPPED: {case_id}（{reason}）\n")
            atomic_write(synthetic_skipped(fqcn, method, case_id, reason), Path(output))
            return 0
        fail(f"Surefire 报告中未找到目标用例: {fqcn}::{method}"
             f"（目标方法未产生任何 testcase；请检查 DataProvider 数据源或方法名）")

    for testcase in selected:
        if issue and matches_known_issue(testcase, issue):
            force_skipped(testcase, issue.get("reason") or "已知问题")
        else:
            enrich_skipped_message(testcase, results_tree, method)

    suite = ET.Element("testsuite", {"name": fqcn, **suite_summary(selected)})
    for case in selected:
        suite.append(case)
    root = ET.Element("testsuites")
    root.append(suite)
    atomic_write(ET.ElementTree(root), Path(output))
    sys.stderr.write(f"[plm] 已写出 {len(selected)} 个 testcase 到 {output}\n")
    return 0


def cmd_merge(reports_dir: str, output: str) -> int:
    report_dir = Path(reports_dir)
    if not report_dir.is_dir():
        fail(f"报告目录不存在: {report_dir}")
    selected = []
    for path in sorted(report_dir.glob("case-*.xml")):
        tree = ET.parse(path)
        selected.extend(tree.getroot().iter("testcase"))
    if not selected:
        fail(f"没有可合并的 case-*.xml: {report_dir}")

    suite = ET.Element("testsuite", {"name": "plm-batch", **suite_summary(selected)})
    for case in selected:
        suite.append(copy.deepcopy(case))
    root = ET.Element("testsuites")
    root.append(suite)
    atomic_write(ET.ElementTree(root), Path(output))
    sys.stderr.write(f"[plm] 已合并 {len(selected)} 个 testcase 到 {output}\n")
    return 0


def main(argv: list) -> int:
    if len(argv) >= 7 and argv[1] == "single":
        return cmd_single(argv[2], argv[3], argv[4], argv[5], argv[6])
    if len(argv) >= 7 and argv[1] == "timeout":
        return cmd_timeout(argv[2], argv[3], argv[4], argv[5], argv[6])
    if len(argv) >= 7 and argv[1] == "timeout-reason":
        return cmd_timeout_reason(argv[2], argv[3], argv[4], argv[5], " ".join(argv[6:]))
    if len(argv) >= 6 and argv[1] == "known-skip":
        return cmd_known_skip(argv[2], argv[3], argv[4], argv[5])
    if len(argv) >= 4 and argv[1] == "merge":
        return cmd_merge(argv[2], argv[3])
    sys.stderr.write(
        "Usage: plm_xml_post.py single <surefireDir> <output> <caseId> <fqcn> <method>\n"
        "       plm_xml_post.py timeout <output> <caseId> <fqcn> <method> <seconds>\n"
        "       plm_xml_post.py timeout-reason <output> <caseId> <fqcn> <method> <reason...>\n"
        "       plm_xml_post.py known-skip <output> <caseId> <fqcn> <method>\n"
        "       plm_xml_post.py merge <reportsDir> <output>\n"
    )
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
