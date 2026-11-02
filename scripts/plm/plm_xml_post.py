#!/usr/bin/env python3
"""PLM 适配工具：把 Surefire/TestNG 报告转换成 PLM 约定的 JUnit XML。

- single: 从 Surefire 报告目录提取指定类/方法的 testcase，注入 case_id 后写目标文件。
- merge:  把同一批次产生的 case-*.xml 合并成 cases.xml（兼容批量入口）。
"""

import copy
import os
import sys
import tempfile
from pathlib import Path
from xml.etree import ElementTree as ET


def fail(message: str) -> None:
    sys.stderr.write(f"[plm] {message}\n")
    raise SystemExit(1)


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
        fail(f"Surefire 报告中未找到目标用例: {fqcn}::{method}")

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
    if len(argv) >= 4 and argv[1] == "merge":
        return cmd_merge(argv[2], argv[3])
    sys.stderr.write(
        "Usage: plm_xml_post.py single <surefireDir> <output> <caseId> <fqcn> <method>\n"
        "       plm_xml_post.py merge <reportsDir> <output>\n"
    )
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
