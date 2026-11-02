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
SUREFIRE_DIR="details/target/surefire-reports-plm"
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
    printf '%s' "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

ensure_built() {
    # 每次 install 都是增量的；同时保证 common 已进入本地仓库，details 可单模块运行。
    mvn -q -DskipTests -pl details -am install || return 1
}

prepare() {
    ensure_built
}

list_cases() {
    "$PYTHON" scripts/plm/plm_cases.py list --automation-type "$AUTOMATION_TYPE"
}

case_one() {
    [ -z "${1:-}" ] && { usage; return 2; }
    normalized=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
    lookup=$("$PYTHON" scripts/plm/plm_cases.py lookup "$normalized") || return 5
    fqcn=$(printf '%s' "$lookup" | cut -f1)
    method=$(printf '%s' "$lookup" | cut -f2)

    mkdir -p "$REPORTS_DIR"
    target="$REPORTS_DIR/case-$(safe_name "$normalized").xml"
    rm -f -- "$target"

    rm -rf -- "$TESTNG_TMP_DIR" "$SUREFIRE_DIR"
    mkdir -p "$TESTNG_TMP_DIR"
    xml="$TESTNG_TMP_DIR/single.xml"
    "$PYTHON" scripts/plm/plm_cases.py testng-xml "$normalized" "$xml" || return 1

    ensure_built_if_needed() {
        if [ ! -d "$REPO_ROOT/details/target/test-classes" ]; then
            ensure_built || {
                echo "[plm] 测试程序编译失败" >&2
                return 1
            }
        fi
    }

    ensure_built_if_needed || return 1

    mvn -q -pl details test \
        -Dsurefire.suiteXmlFiles="$REPO_ROOT/$xml" \
        -Dsurefire.reportsDirectory="$REPO_ROOT/$SUREFIRE_DIR" \
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
