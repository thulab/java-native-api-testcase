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
#
# 同类复用：PLM 注入 PLM_RUN_ID 时，同一 run 内同一个 TestNG 类只真正执行一次，
# 其余用例从缓存报告里提取；PLM_CLASS_CACHE=0 可关闭（手工排查时用）。
# 单类硬超时：PLM_CLASS_TIMEOUT_SEC（默认 600 秒），超时写 ERROR 结果而不是挂住整轮。
# ============================================================================
set -u

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
cd "$REPO_ROOT" || exit 1

REPORTS_DIR="${PLM_REPORT_DIR:-reports}"
AUTOMATION_TYPE="${PLM_AUTOMATION_TYPE:-session-java}"
PYTHON="${PYTHON:-python3}"
SUREFIRE_DIR="details/target/surefire-reports"
TESTNG_TMP_DIR="details/target/plm-testng"
BUILD_MARKER="$REPO_ROOT/details/target/.plm-build-ok"
CP_FILE="$REPO_ROOT/details/target/plm-classpath.txt"
TESTNG_OUT="$REPO_ROOT/details/target/plm-testng/out"

# 同一 PLM run 内按“测试类”复用整类执行结果。
# PLM 逐条派发时如果每条都重跑整个 TestNG 类，142 行的 DataProvider 会被重放十几次，
# 单机 K8s 实例的 region 分配跟不上，表现为 906/505/508 级联。这里以
# runId + FQCN + 代码版本 + 构建标记 + 数据库地址 为键缓存整类报告，后续用例只做提取。
# 仅当 PLM 注入 PLM_RUN_ID 时启用；手工执行或 PLM_CLASS_CACHE=0 时保持逐条真跑。
# 注意：PLM_REPORT_DIR 是逐条用例变化的（reports/plm-exec/<时间戳>），
# 类缓存必须放在跨用例稳定的目录里，否则永远不会命中。reports/ 已被 .gitignore 忽略。
CLASS_CACHE_ROOT="${PLM_CLASS_CACHE_DIR:-$REPO_ROOT/reports/plm-class-cache}"
CLASS_CACHE_RUN_ID="${PLM_RUN_ID:-}"
if [ "${PLM_CLASS_CACHE:-1}" = "0" ]; then
    CLASS_CACHE_RUN_ID=""
fi
CLASS_CACHE_RUN_KEY=$(printf '%s' "$CLASS_CACHE_RUN_ID" | tr -c 'A-Za-z0-9._-' '_')
# 防御性处理：run id 只应是数字，但 "."/".." 会逃出缓存根目录。
if [ "$CLASS_CACHE_RUN_KEY" = "." ] || [ "$CLASS_CACHE_RUN_KEY" = ".." ]; then
    CLASS_CACHE_RUN_KEY="_"
fi
# 单个测试类的硬超时：TestNG/客户端可能卡在服务端 socket 读上（无读超时），
# 不设上限会吃掉 PLM 整轮时限。默认 10 分钟，可用 PLM_CLASS_TIMEOUT_SEC 覆盖。
CLASS_TIMEOUT_SEC="${PLM_CLASS_TIMEOUT_SEC:-600}"

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

class_cache_dir() {
    # $1 = FQCN。键里必须带代码版本/构建标记/DB 地址：
    # 程序更新或换实例后不能复用旧结果。
    fqcn_safe=$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_')
    head=$(git -C "$REPO_ROOT" rev-parse --short=12 HEAD 2>/dev/null || echo nogit)
    # 本地改动也进键：避免 git checkout 保留旧 mtime 时 needs_build 漏判。
    dirty=$(git -C "$REPO_ROOT" status --porcelain --untracked-files=no 2>/dev/null | cksum | cut -d' ' -f1)
    build=$(stat -c %Y "$BUILD_MARKER" 2>/dev/null || stat -f %m "$BUILD_MARKER" 2>/dev/null || echo 0)
    db=$(printf '%s_%s' "${IOTDB_HOST:-}" "${IOTDB_PORT:-}" | tr -c 'A-Za-z0-9._-' '_')
    dep=$(printf '%s' "${DEP_VERSION:-none}" | tr -c 'A-Za-z0-9._-' '_')
    printf '%s/%s/%s/%s-%s-%s-%s-%s' \
        "$CLASS_CACHE_ROOT" "$CLASS_CACHE_RUN_KEY" "$fqcn_safe" \
        "$head" "$dirty" "$build" "$db" "$dep"
}

cleanup_class_cache() {
    [ -n "$CLASS_CACHE_RUN_KEY" ] || return 0
    [ -d "$CLASS_CACHE_ROOT" ] || return 0
    # 只按年龄回收旧 run；当前 run 的目录保持不动。
    find "$CLASS_CACHE_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime +2 \
        -exec rm -rf -- {} + 2>/dev/null || true
}

cleanup_old_reports() {
    # PLM_REPORT_DIR 是逐条用例的新目录；执行机上 reports/plm-exec 会持续累积。
    # 产物已由 PLM 下载归档，这里按年龄回收，避免长期占满执行机磁盘。
    for dir in "$REPO_ROOT/reports/plm-exec" "$REPO_ROOT/reports/plm-run-state"; do
        [ -d "$dir" ] || continue
        find "$dir" -mindepth 1 -maxdepth 1 -type d -mtime +2 \
            -exec rm -rf -- {} + 2>/dev/null || true
    done
}

class_cache_hit() {
    # $1 cache dir, $2 target, $3 normalized, $4 fqcn, $5 method
    if [ -f "$1/.timeout" ]; then
        reason=$(cat "$1/.timeout" 2>/dev/null)
        "$PYTHON" scripts/plm/plm_xml_post.py timeout-reason \
            "$2" "$3" "$4" "$5" "$reason" 2>/dev/null
        return $?
    fi
    [ -f "$1/.complete" ] || return 1
    [ -d "$1/junitreports" ] || return 1
    "$PYTHON" scripts/plm/plm_xml_post.py single \
        "$1/junitreports" "$2" "$3" "$4" "$5" 2>/dev/null
}

class_cache_save_timeout() {
    # $1 cache dir, $2 原因。同一 run 内同类后续用例直接复用该超时结论。
    tmp="$1.tmp.$$"
    rm -rf -- "$tmp" || return 1
    mkdir -p "$tmp" || return 1
    printf '%s' "$2" > "$tmp/.timeout" || {
        rm -rf -- "$tmp"
        return 1
    }
    rm -rf -- "$1"
    mv "$tmp" "$1" || {
        rm -rf -- "$tmp"
        return 1
    }
    return 0
}

class_cache_save() {
    # $1 cache dir
    tmp="$1.tmp.$$"
    rm -rf -- "$tmp" || return 1
    mkdir -p "$tmp" || return 1
    cp -R "$TESTNG_OUT/junitreports" "$tmp/junitreports" || {
        rm -rf -- "$tmp"
        return 1
    }
    if [ -f "$TESTNG_OUT/testng-results.xml" ]; then
        cp "$TESTNG_OUT/testng-results.xml" "$tmp/testng-results.xml" || {
            rm -rf -- "$tmp"
            return 1
        }
    fi
    touch "$tmp/.complete" || {
        rm -rf -- "$tmp"
        return 1
    }
    rm -rf -- "$1"
    mv "$tmp" "$1" || {
        rm -rf -- "$tmp"
        return 1
    }
    return 0
}

class_run_cacheable() {
    # $1 = java 退出码。类超时（rc=124）不缓存；配置方法失败由 testng-results.xml 的
    # is-config 检查兜底（TestNG 退出码是位掩码，3 只表示“有失败+有跳过”，不能据此判配置失败）。
    # 普通用例失败/跳过仍缓存，保证同一 run 内结果一致。
    [ "$1" -ne 124 ] || return 1
    [ -d "$TESTNG_OUT/junitreports" ] || return 1
    [ -f "$TESTNG_OUT/testng-results.xml" ] || return 1
    grep -Rq '<testcase' "$TESTNG_OUT/junitreports" 2>/dev/null || return 1
    if grep -Eq 'is-config="true"[^>]*status="FAIL"|status="FAIL"[^>]*is-config="true"' \
        "$TESTNG_OUT/testng-results.xml" 2>/dev/null; then
        return 1
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

    # 已知会挂起/无法执行的用例（.plm-known-issues.json 的 skipRun）：
    # 不启动测试类，直接写带原因的 SKIPPED，避免每条都等类超时。
    if "$PYTHON" scripts/plm/plm_xml_post.py known-skip \
        "$target" "$normalized" "$fqcn" "$method"; then
        return 0
    fi

    # 构建结果按源码时间戳缓存：PLM 逐条派发时只做一次 install/test-compile/classpath，
    # 后续用例直接用 java + TestNG 运行单条，避免 379 条用例重复启动 Maven 把批次拖过总时限。
    # 必须在计算类缓存键之前执行：程序更新后 BUILD_MARKER 会变，键随之变化，不会复用旧结果。
    ensure_built_cached || {
        echo "[plm] 测试程序编译失败" >&2
        return 1
    }

    [ -f "$CP_FILE" ] || {
        mvn_dep -o -q -pl details dependency:build-classpath -Dmdep.outputFile="$CP_FILE" || return 1
    }

    cache_dir=""
    if [ -n "$CLASS_CACHE_RUN_KEY" ]; then
        cleanup_old_reports
        cleanup_class_cache
        cache_dir=$(class_cache_dir "$fqcn")
        if class_cache_hit "$cache_dir" "$target" "$normalized" "$fqcn" "$method"; then
            echo "[plm] 复用本轮已执行的测试类结果: $fqcn" >&2
            return 0
        fi
        # 缓存里没有目标方法（类执行被跳过、数据源为空、或上次是配置失败）：
        # 作废该缓存并重新执行一次，避免把一次坏结果放大到整个类。
        rm -rf -- "$cache_dir"
    fi

    rm -rf -- "$TESTNG_TMP_DIR" "$SUREFIRE_DIR"
    mkdir -p "$TESTNG_TMP_DIR"
    xml="$TESTNG_TMP_DIR/single.xml"
    "$PYTHON" scripts/plm/plm_cases.py testng-xml "$normalized" "$xml" || return 1

    # 直接用 TestNG 运行整类，避免每条用例启动一次 Maven。
    classpath="$REPO_ROOT/details/target/test-classes:$REPO_ROOT/common/target/classes:$(cat "$CP_FILE")"
    rm -rf -- "$TESTNG_OUT"
    if command -v timeout >/dev/null 2>&1; then
        timeout -k 30 "$CLASS_TIMEOUT_SEC" java -ea -cp "$classpath" org.testng.TestNG "$xml" -d "$TESTNG_OUT"
    else
        echo "[plm] 警告：未找到 timeout 命令，测试类不会被硬超时终止（请安装 coreutils/timeout）" >&2
        java -ea -cp "$classpath" org.testng.TestNG "$xml" -d "$TESTNG_OUT"
    fi
    rc=$?

    timeout_reason=""
    if [ "$rc" -eq 124 ]; then
        timeout_reason="测试类执行超时（${CLASS_TIMEOUT_SEC}s），已终止；请检查服务端是否卡住或连接是否可用"
        if [ -n "$cache_dir" ]; then
            class_cache_save_timeout "$cache_dir" "$timeout_reason" || \
                echo "[plm] 超时结果缓存失败（不影响本次结果）" >&2
        fi
        # 类被 timeout 杀掉时报告可能只写了一半，不能拿部分 testcase 判通过/失败；
        # 直接写明确的超时 ERROR，下个 run 重新执行。
        "$PYTHON" scripts/plm/plm_xml_post.py timeout-reason \
            "$target" "$normalized" "$fqcn" "$method" "$timeout_reason" || {
            rm -f -- "$target"
            return 1
        }
        return $rc
    fi

    if [ -n "$cache_dir" ] && class_run_cacheable "$rc"; then
        class_cache_save "$cache_dir" || echo "[plm] 测试类结果缓存失败（不影响本次结果）" >&2
    fi

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
