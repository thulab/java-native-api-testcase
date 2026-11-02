#!/bin/sh
# ============================================================================
# java-native-api-test 的 PLM 统一入口
#
# 测试框架：TestNG + Maven Surefire
# PLM 调用：sh start.sh case '<CASE_ID>'   （cwd = 仓库根）
# 产物：$PLM_REPORT_DIR/case-<ID>.xml（JUnit XML，每个 testcase 带 case_id）
#
# 数据库连接通过环境变量覆盖（不设置时沿用 config.properties）：
#   IOTDB_HOST / IOTDB_PORT / IOTDB_USER / IOTDB_PASSWORD
#   IOTDB_IS_CLUSTER / IOTDB_HOST_NODES
# ============================================================================
set -u

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
cd "$REPO_ROOT" || exit 1

REPORTS_DIR="${PLM_REPORT_DIR:-reports}"
AUTOMATION_TYPE="${PLM_AUTOMATION_TYPE:-session-java}"
PYTHON="${PYTHON:-python3}"
SUREFIRE_DIR="details/target/surefire-reports"
TESTNG_TMP_DIR="details/target/plm-testng"

usage() {
    cat >&2 <<'EOF'
Usage: sh start.sh <command> [target]
  prepare              编译测试程序（mvn install -DskipTests）
  list-cases --json    输出用例清单 JSON（stdout 仅 JSON）
  case <case-id>       执行单条用例
  cases <id1,id2>      批量执行（兼容保留；PLM 只逐条派发）
  help                 帮助
EOF
}

safe_name() {
    # Java 用例编号（FQCN::method / PLM 编号）不含反斜杠，只需防止 '/'
    # 造成路径穿越；保留大小写与 : @ 等字符，文件名保持 case-<原始 ID>.xml。
    printf '%s' "$1" | tr '/' '_'
}

ensure_built() {
    # 先安装根 POM（details 单模块构建需要 parent:master），再安装 common；
    # details 本身由 mvn test/test-compile 编译，避免每条用例触发 package/shade。
    mvn -q -N -DskipTests install || return 1
    mvn -q -DskipTests -pl common install || return 1
}

prepare() {
    ensure_built && mvn -q -DskipTests -pl details test-compile
}

list_cases() {
    "$PYTHON" scripts/plm/plm_cases.py list --automation-type "$AUTOMATION_TYPE"
}

require_plm_iotdb_endpoint() {
    # PLM 执行时必须使用平台从 K8s 实例解析出的地址；不要回退到 config.properties
    # 的 127.0.0.1，否则测试会悄悄连到执行机本地数据库。
    if [ -n "${PLM_AUTOMATION_TYPE:-}" ]; then
        if [ -z "${IOTDB_HOST:-}" ] || [ -z "${IOTDB_PORT:-}" ]; then
            echo "[plm] PLM 执行必须提供 K8s 实例连接地址 IOTDB_HOST/IOTDB_PORT" >&2
            return 2
        fi
    fi
    return 0
}

case_one() {
    [ -z "${1:-}" ] && { usage; return 2; }
    require_plm_iotdb_endpoint || return 2
    normalized=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
    lookup=$("$PYTHON" scripts/plm/plm_cases.py lookup "$normalized") || return 5
    fqcn=$(printf '%s' "$lookup" | cut -f1)
    method=$(printf '%s' "$lookup" | cut -f2)

    mkdir -p "$REPORTS_DIR"
    target="$REPORTS_DIR/case-$(safe_name "$1").xml"
    rm -f -- "$target"

    rm -rf -- "$TESTNG_TMP_DIR" "$SUREFIRE_DIR"
    mkdir -p "$TESTNG_TMP_DIR"
    xml="$TESTNG_TMP_DIR/single.xml"
    "$PYTHON" scripts/plm/plm_cases.py testng-xml "$normalized" "$xml" || return 1

    # 每次执行都做增量 install：保证 common 变更（含 ReadConfig 环境变量支持）进入
    # 本地仓库；只检查 details/target/test-classes 会让 common 更新被静默跳过。
    ensure_built || {
        echo "[plm] 测试程序编译失败" >&2
        return 1
    }

    mvn -q -pl details test \
        -Dsurefire.suiteXmlFiles="$REPO_ROOT/$xml" \
        -DfailIfNoTests=false
    rc=$?

    if ! "$PYTHON" scripts/plm/plm_xml_post.py single \
        "$SUREFIRE_DIR" "$target" "$normalized" "$fqcn" "$method"; then
        rm -f -- "$target"
        return 1
    fi
    return $rc
}

cases_batch() {
    [ -z "${1:-}" ] && { usage; return 2; }
    ok=0
    old_ifs=$IFS
    IFS=','
    for raw in $1; do
        id=$(printf '%s' "$raw" | tr -d '[:space:]')
        [ -z "$id" ] && continue
        case_one "$id" || ok=1
    done
    IFS=$old_ifs
    "$PYTHON" scripts/plm/plm_xml_post.py merge "$REPORTS_DIR" "$REPORTS_DIR/cases.xml" || return 1
    return $ok
}

command="${1:-help}"
target="${2:-}"
case "$command" in
    prepare) prepare ;;
    list-cases)
        [ "$target" = "--json" ] || { usage; exit 2; }
        list_cases
        ;;
    case) case_one "$target" ;;
    cases) cases_batch "$target" ;;
    help|-h|--help) usage ;;
    *) usage; exit 2 ;;
esac
