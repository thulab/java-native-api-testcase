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
#
# 依赖版本覆盖：PLM 产品源码构建把本轮 TimechoDB 版本写入仓库根的
# .plm-dep-version（未跟踪文件），本脚本读取后用 -Diotdb.version 覆盖根 pom
# 的声明，保证消费的是本轮构建产物；文件不存在时沿用 pom 里的版本。
# ============================================================================
set -u

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
cd "$REPO_ROOT" || exit 1

REPORTS_DIR="${PLM_REPORT_DIR:-reports}"
AUTOMATION_TYPE="${PLM_AUTOMATION_TYPE:-session-java}"
PYTHON="${PYTHON:-python3}"
SUREFIRE_DIR="details/target/surefire-reports"
TESTNG_TMP_DIR="details/target/plm-testng"

# PLM 产品源码构建写入的依赖版本（未跟踪文件，见文件头说明）
DEP_VERSION_FILE="$REPO_ROOT/.plm-dep-version"
DEP_VERSION=""
if [ -f "$DEP_VERSION_FILE" ]; then
    DEP_VERSION=$(sed -n '1p' "$DEP_VERSION_FILE" | tr -d '[:space:]')
fi

# 所有需要解析 iotdb.version 的 Maven 调用统一走这里
mvn_dep() {
    if [ -n "$DEP_VERSION" ]; then
        mvn "$@" "-Diotdb.version=$DEP_VERSION"
    else
        mvn "$@"
    fi
}

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
    mvn_dep -q -N -DskipTests install || return 1
    mvn_dep -q -DskipTests -pl common install || return 1
}

BUILD_MARKER="$REPO_ROOT/details/target/.plm-build-ok"
CP_FILE="$REPO_ROOT/details/target/plm-classpath.txt"
TESTNG_OUT="$REPO_ROOT/details/target/plm-testng/out"

needs_build() {
    [ -f "$BUILD_MARKER" ] || return 0
    [ -d "$REPO_ROOT/common/target/classes" ] || return 0
    [ -d "$REPO_ROOT/details/target/test-classes" ] || return 0
    [ -f "$DEP_VERSION_FILE" ] && [ "$DEP_VERSION_FILE" -nt "$BUILD_MARKER" ] && return 0
    [ "$REPO_ROOT/pom.xml" -nt "$BUILD_MARKER" ] && return 0
    [ "$REPO_ROOT/common/pom.xml" -nt "$BUILD_MARKER" ] && return 0
    [ "$REPO_ROOT/details/pom.xml" -nt "$BUILD_MARKER" ] && return 0
    find "$REPO_ROOT/common/src" "$REPO_ROOT/details/src" -type f -newer "$BUILD_MARKER" 2>/dev/null | grep -q . && return 0
    return 1
}

ensure_built_cached() {
    needs_build || return 0
    ensure_built || return 1
    mvn_dep -q -DskipTests -pl details test-compile || return 1
    mvn_dep -o -q -pl details dependency:build-classpath -Dmdep.outputFile="$CP_FILE" || return 1
    touch "$BUILD_MARKER"
}

prepare() {
    ensure_built_cached && [ -f "$CP_FILE" ] || {
        # marker 已存在但 classpath 文件被清理时补生成
        mvn_dep -o -q -pl details dependency:build-classpath -Dmdep.outputFile="$CP_FILE" || return 1
    }
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

    # 构建结果按源码时间戳缓存：PLM 逐条派发时只做一次 install/test-compile/classpath，
    # 后续用例直接用 java + TestNG 运行单条，避免 379 条用例重复启动 Maven 把批次拖过总时限。
    ensure_built_cached || {
        echo "[plm] 测试程序编译失败" >&2
        return 1
    }

    [ -f "$CP_FILE" ] || {
        mvn_dep -o -q -pl details dependency:build-classpath -Dmdep.outputFile="$CP_FILE" || return 1
    }

    # 直接用 TestNG 运行单条用例，避免每条用例启动一次 Maven。
    classpath="$REPO_ROOT/details/target/test-classes:$REPO_ROOT/common/target/classes:$(cat "$CP_FILE")"
    rm -rf -- "$TESTNG_OUT"
    java -ea -cp "$classpath" org.testng.TestNG "$xml" -d "$TESTNG_OUT"
    rc=$?

    if ! "$PYTHON" scripts/plm/plm_xml_post.py single \
        "$TESTNG_OUT/junitreports" "$target" "$normalized" "$fqcn" "$method"; then
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
