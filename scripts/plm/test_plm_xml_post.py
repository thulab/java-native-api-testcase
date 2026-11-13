#!/usr/bin/env python3
"""plm_xml_post.py 的单测：跳过原因回填、已知问题转换、空目标与超时结果。

运行：python3 -m unittest discover -s scripts/plm -p 'test_*.py'
"""

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent))
import plm_xml_post as post  # noqa: E402


class PlmXmlPostTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.reports = self.root / "out" / "junitreports"
        self.reports.mkdir(parents=True)
        self.known = self.root / "known.json"
        self.known.write_text(json.dumps({
            "TIMECHODB-2489": {"reason": "empty data provider"},
            "TIMECHODB-2469": {"reason": "known duplicate", "match": "should have thrown"},
            "TIMECHODB-2538": {"reason": "known hang", "skipRun": True},
            "TIMECHODB-2539": {"reason": "hang but must run", "timeoutSec": 1200},
        }), encoding="utf-8")
        self.original_known = post.KNOWN_ISSUES_FILE
        post.KNOWN_ISSUES_FILE = self.known

    def tearDown(self):
        post.KNOWN_ISSUES_FILE = self.original_known
        self.tmp.cleanup()

    def write_report(self, testcases):
        suite = ET.Element("testsuite", {"name": "Demo"})
        for testcase in testcases:
            suite.append(testcase)
        root = ET.Element("testsuites")
        root.append(suite)
        ET.ElementTree(root).write(self.reports / "TEST-Demo.xml", encoding="utf-8")

    def _testcase(self, name, child=None):
        testcase = ET.Element("testcase", {"classname": "Demo", "name": name, "time": "0.1"})
        if child is not None:
            testcase.append(child)
        return testcase

    def output(self):
        return self.root / "case.xml"

    def test_skipped_message_from_config_failure(self):
        self.write_report([self._testcase("testSkip", ET.Element("skipped"))])
        (self.root / "out" / "testng-results.xml").write_text(
            '<testng-results><test-method is-config="true" name="setup" status="FAIL">'
            '<exception><message>setup boom</message></exception></test-method></testng-results>',
            encoding="utf-8")
        code = post.cmd_single(str(self.reports), str(self.output()), "CASE-1", "Demo", "testSkip")
        self.assertEqual(0, code)
        skipped = ET.parse(self.output()).getroot().find(".//skipped")
        self.assertEqual("配置方法 setup 失败: setup boom", skipped.get("message"))

    def test_known_issue_converts_matching_error(self):
        error = ET.Element("error", {"message": "Method x should have thrown an exception"})
        self.write_report([self._testcase("testKnown", error)])
        code = post.cmd_single(str(self.reports), str(self.output()), "TIMECHODB-2469", "Demo", "testKnown")
        self.assertEqual(0, code)
        skipped = ET.parse(self.output()).getroot().find(".//skipped")
        self.assertIn("known duplicate", skipped.get("message"))

    def test_known_issue_does_not_convert_other_error(self):
        error = ET.Element("error", {"message": "different failure"})
        self.write_report([self._testcase("testKnown", error)])
        code = post.cmd_single(str(self.reports), str(self.output()), "TIMECHODB-2469", "Demo", "testKnown")
        self.assertEqual(0, code)
        self.assertIsNotNone(ET.parse(self.output()).getroot().find(".//error"))

    def test_no_match_known_issue_does_not_hide_real_error(self):
        # 2489 没有 match：只能靠 skipRun 跳过；真实错误必须保留。
        error = ET.Element("error", {"message": "a real regression"})
        self.write_report([self._testcase("testKnown", error)])
        code = post.cmd_single(str(self.reports), str(self.output()), "TIMECHODB-2489", "Demo", "testKnown")
        self.assertEqual(0, code)
        self.assertIsNotNone(ET.parse(self.output()).getroot().find(".//error"))

    def test_known_issue_without_target_fails(self):
        # 没有 match 也没有 skipRun 的条目不能把“目标方法 0 次执行”吞成 SKIPPED。
        self.write_report([])
        with self.assertRaises(SystemExit) as raised:
            post.cmd_single(str(self.reports), str(self.output()), "TIMECHODB-2489", "Demo", "missing")
        self.assertEqual(1, raised.exception.code)

    def test_timeout_sec_case_without_target_fails(self):
        # 配置了 timeoutSec 的用例必须真实执行；目标方法 0 次执行是执行故障，不能报 SKIPPED。
        self.write_report([])
        with self.assertRaises(SystemExit) as raised:
            post.cmd_single(str(self.reports), str(self.output()), "TIMECHODB-2539", "Demo", "missing")
        self.assertEqual(1, raised.exception.code)

    def test_matching_known_issue_without_target_fails(self):
        # 2469 带 match：没有真实错误消息时不能合成已知问题跳过。
        self.write_report([])
        with self.assertRaises(SystemExit) as raised:
            post.cmd_single(str(self.reports), str(self.output()), "TIMECHODB-2469", "Demo", "missing")
        self.assertEqual(1, raised.exception.code)

    def test_unknown_missing_target_fails(self):
        self.write_report([])
        with self.assertRaises(SystemExit) as raised:
            post.cmd_single(str(self.reports), str(self.output()), "CASE-9", "Demo", "missing")
        self.assertEqual(1, raised.exception.code)

    def test_known_skip_run_writes_skip(self):
        code = post.cmd_known_skip(str(self.output()), "TIMECHODB-2538", "Demo", "testA")
        self.assertEqual(0, code)
        skipped = ET.parse(self.output()).getroot().find(".//skipped")
        self.assertIn("known hang", skipped.get("message"))

    def test_known_skip_run_ignores_unlisted_case(self):
        self.assertEqual(2, post.cmd_known_skip(str(self.output()), "TIMECHODB-9999", "Demo", "testA"))

    def _timeout_sec(self, case_id):
        buffer = io.StringIO()
        original = sys.stdout
        sys.stdout = buffer
        try:
            self.assertEqual(0, post.cmd_timeout_sec(case_id))
        finally:
            sys.stdout = original
        return buffer.getvalue()

    def test_timeout_sec_reads_configured_value(self):
        self.assertEqual("1200", self._timeout_sec("TIMECHODB-2539"))

    def test_timeout_sec_ignores_unlisted_case(self):
        self.assertEqual("", self._timeout_sec("TIMECHODB-9999"))

    def test_timeout_sec_ignores_skip_run_case(self):
        # 2538 只有 skipRun，没有 timeoutSec：不输出超时值。
        self.assertEqual("", self._timeout_sec("TIMECHODB-2538"))

    def test_timeout_writes_error(self):
        code = post.cmd_timeout(str(self.output()), "CASE-1", "Demo", "testA", "120")
        self.assertEqual(0, code)
        error = ET.parse(self.output()).getroot().find(".//error")
        self.assertIn("120s", error.get("message"))


if __name__ == "__main__":
    unittest.main()
