#!/usr/bin/env bash
# 工业形态 JMH 基准一键运行 + 多线程扩展曲线自动汇总（72 核 / 1GB 驻留旗舰档）。
#
# 覆盖：
#   1) 纯读 Zipf 读路径线程扩展（HotReadZipfBenchmark.read，旗舰 1GB 驻留：keySpace=2^26/maxSize=2^23）
#   2) 1GB 驻留读写混合（HotReadZipfBenchmark.mixed95_5）
#   3) 写密集 + 驱逐（WriteEvictionBenchmark.putCycling：小驻留 wt 扫描 + 旗舰 1GB 行）
#   4) TTL 过期 + 后台清扫压力（TtlExpirySweepBenchmark.putUpdates，小驻留：清扫 O(驻留)）
#   5) 读穿单飞去重（SingleFlightLoadBenchmark）
#   6) footprint 内存探测（FootprintProbe，非 JMH）
#   7) 引擎微基准（EngineThroughputBenchmark，-t 4）
#   8) size() 吞吐（CacheSizeThroughputBenchmark，ops/s：规模/开销/线程扩展）
#
# 对 "-t"（读）与 "writerThreads"（写）的扫描结果自动解析 JMH 的 Result/诊断行并整理成
# markdown 表写入 $ROOT/industrial_scale_report.md，同时打印到终端。
#
# 用法：
#   bash run_industrial.sh          # 完整矩阵（本机 72 核约 20–40 分钟）
#   QUICK=1 bash run_industrial.sh  # 冒烟：小键空间 + 每个基准仅 1 轮短迭代（约 2–3 分钟）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$ROOT/modules/lcache-benchmark/target/lcache-benchmarks.jar"
REPORT="$ROOT/industrial_scale_report.md"

# 运行日志放临时目录，避免污染工作区（*.log 已 gitignore，但仍集中管理）
RUN_DIR="$(mktemp -d)"
trap 'rm -rf "$RUN_DIR"' EXIT

# QUICK=1 → 每个基准仅 1 轮 300ms 冒烟；否则走稳定矩阵旗标。
# 旗标用数组而非字符串：若用 "…" 字符串展开会被当单个 argv，JMH 报
# "Cannot parse argument '…' of option w"。各调用处再统一追加 "-f 1"，此处不含 -f。
if [ -n "${QUICK:-}" ]; then
  W_FLAGS=(-wi 1 -w 200ms -i 1 -r 300ms)
else
  # 统一稳定旗标：显式 warmup/measurement、显式 -t、-p 收敛矩阵（-f 1 见各调用处）
  W_FLAGS=(-wi 3 -w 1s -i 5 -r 1s)
fi

# 数据量档位：全量 = 旗舰 1GB 驻留（keySpace 2^26 / maxSize 2^23，INTEGER ~132B/条 ≈1GB）；
# QUICK = 小键空间快速冒烟。
if [ -n "${QUICK:-}" ]; then
  KS=65536        # 键空间 2^16
  MX=16384        # 驻留 2^14
  SZ_SCALE=65536          # size() 规模/线程扫描的默认 scale
  SZ_SCALES="65536 262144"  # size() 规模两档
  SIZE_TS="1 4 16"        # size() 线程扩展
  export JAVA_TOOL_OPTIONS="-Xms1g -Xmx6g"
else
  KS=67108864     # 键空间 2^26
  MX=8388608      # 驻留 2^23 ≈1GB
  SZ_SCALE=1048576        # size() 默认 scale = 1M
  SZ_SCALES="1048576 10485760"  # 1M / 10M
  SIZE_TS="1 2 4 8 16 32 64"
  export JAVA_TOOL_OPTIONS="-Xms2g -Xmx16g"
fi

# ── 解析工具 ─────────────────────────────────────────────────────────────
# JMH 每个 benchmark 结束后打印:
#   Result "io.lcache...method":
#     1492.501 ±(99.9%) 12.3 ops/ms
score_of() { # stdin: java 输出（stdout+stderr 合并）
  # 分数行以 "  123.4 ±(...) N ops/ms" 开头；多轮测量时 JMH 会在行尾追加 " [Average]"，
  # 故不能用 /ops\/ms$/ 锚定行尾，只要求该行含 ops/ms。
  awk '/^Result "/{inres=1}
       inres && /^  [0-9]/ && /ops\/ms/ {print $1; exit}'
}
score_of_s() { # 与 score_of 同理，但匹配 ops/s（CacheSizeThroughputBenchmark 用 TimeUnit.SECONDS）
  awk '/^Result "/{inres=1}
       inres && /^  [0-9]/ && /ops\/s/ {print $1; exit}'
}
field_of() { # $1=日志文件 $2=字段名(如 hitRate / expires / evictions / dedup)
  grep -oE "$2=[0-9.]+" "$1" | head -1 | cut -d= -f2
}

# 循环内跑单配置 JMH：$1=日志文件，其余为 java 参数。
# 失败不中断脚本（保留 set -e 对其它真错误的敏感性），但打 [warn] 到 stderr，表格对应格为 "-"。
jrun() {
  local log="$1"; shift
  if ! java -jar "$JAR" "$@" >"$log" 2>&1; then
    echo "      [warn] JMH run failed: $*  (log=$log)" >&2
    tail -20 "$log" >&2 || true
  fi
}

echo "== 1) build =="
mvn -q -pl modules/lcache-benchmark -am package
: > "$REPORT"

{
  echo "# lcache 工业形态基准汇总（$(date '+%Y-%m-%d %H:%M')）"
  echo
  echo "- 机器：JDK 17.0.15 · Intel Xeon Gold 6140 2.30GHz（HT：36 物理 / 72 逻辑核）· 172GB RAM"
  echo "- 旗标: \`${W_FLAGS[*]}\` · fork 堆 \`$JAVA_TOOL_OPTIONS\`"
  echo "- 数据量档位：keySpace=${KS} / maxSize=${MX}（旗舰 ≈1GB INTEGER 驻留；QUICK 用 2^16/2^14）"
  echo "- 详细逐行日志: JMH 输出已解析入下表；诊断行(\`[hot-read]\`/\`[write-evict]\`/"
  echo "  \`[ttl-sweep]\`/\`[single-flight]\`) 见各基准的 TearDown 输出（QUICK/完整跑打印到终端）。"
  echo
} >> "$REPORT"

# ── 2) 读路径线程扩展：纯读 read @ 旗舰 1GB 驻留，-t 1/8/16/32/64 ─────────────
echo "== 2) 读路径线程扩展扫描（HotReadZipfBenchmark.read · 1GB 驻留 -t 1/8/16/32/64） =="
{
  echo "## 读路径线程扩展（HotReadZipfBenchmark.read, keyModel=INT, maxSize=${MX}, keySpace=${KS}）"
  echo
  echo "> 纯读（0% 写，不吃写池）→ 读吞吐随 -t 扩展；hitRate ≈ 理论 ln(maxSize)/ln(keySpace)"
  echo "> 本档 ln2^23/ln2^26 ≈ 0.885（maxSize 与 keySpace 均为 2 的幂时 = 23/26）。"
  echo
  echo "| 引擎 | 指标 | -t 1 | -t 8 | -t 16 | -t 32 | -t 64 |"
  echo "|---|---|---|---|---|---|---|"
} >> "$REPORT"

declare -A READ_SCORE READ_HR
THREADS="1 8 16 32 64"
for eng in LOCK_FREE SYNCHRONIZED; do
  for t in $THREADS; do
    printf "  [%s -t %s] ...\n" "$eng" "$t" >&2
    f="$RUN_DIR/read_${eng}_${t}.log"
    jrun "$f" io.lcache.benchmark.HotReadZipfBenchmark.read \
      -p engine="$eng" -p keyModel=INT -p maxSize="$MX" -p keySpace="$KS" -t "$t" "${W_FLAGS[@]}" -f 1
    READ_SCORE["${eng}_${t}"]="$(score_of <"$f")"
    READ_HR["${eng}_${t}"]="$(field_of "$f" hitRate)"
  done
  {
    printf "| %s | ops/ms |" "$eng"
    for t in $THREADS; do printf " %s |" "${READ_SCORE[${eng}_${t}]:--}"; done
    echo
    printf "| %s | hitRate |" "$eng"
    for t in $THREADS; do printf " %s |" "${READ_HR[${eng}_${t}]:--}"; done
    echo
  } >> "$REPORT"
done

# ── 3) 1GB 驻留读写混合：mixed95_5（5% 写经写池） ────────────────────────────
echo "== 3) 1GB 驻留读写混合（HotReadZipfBenchmark.mixed95_5, -t 16） =="
{
  echo "## 1GB 驻留读写混合（HotReadZipfBenchmark.mixed95_5 · 95%读/5%写 · maxSize=${MX} · keySpace=${KS} · -t 16）"
  echo
  echo "| 引擎 | ops/ms | hitRate |"
  echo "|---|---|---|"
} >> "$REPORT"
for eng in LOCK_FREE SYNCHRONIZED; do
  printf "  [%s mixed95_5 1GB] ...\n" "$eng" >&2
  f="$RUN_DIR/mix_${eng}.log"
  jrun "$f" io.lcache.benchmark.HotReadZipfBenchmark.mixed95_5 \
    -p engine="$eng" -p keyModel=INT -p maxSize="$MX" -p keySpace="$KS" -t 16 "${W_FLAGS[@]}" -f 1
  {
    printf "| %s | %s | %s |\n" "$eng" "$(score_of <"$f")" "$(field_of "$f" hitRate)"
  } >> "$REPORT"
done

# ── 4) 写密集：小驻留 writerThreads 扫描 + 旗舰 1GB putCycling 行 ─────────────
echo "== 4) 写密集 writerThreads 扫描（putCycling, maxSize=16384, -t 8）+ 旗舰 1GB 行 =="
{
  echo "## 写密集 writerThreads 扩展（WriteEvictionBenchmark.putCycling, keyModel=INT, maxSize=16384, -t 8）"
  echo
  echo "> 小驻留档隔离驱逐/写池成本；另附一行旗舰 1GB 驻留的 putCycling（驱逐校验 evictions≈puts）。"
  echo
  echo "| 引擎 | 指标 | wt=1 | wt=4 | wt=8 |"
  echo "|---|---|---|---|---|"
} >> "$REPORT"

declare -A WT_SCORE WT_EVICT
WRITERS="1 4 8"
for eng in LOCK_FREE SYNCHRONIZED; do
  for wt in $WRITERS; do
    printf "  [%s wt=%s] ...\n" "$eng" "$wt" >&2
    f="$RUN_DIR/wt_${eng}_${wt}.log"
    jrun "$f" io.lcache.benchmark.WriteEvictionBenchmark.putCycling \
      -p engine="$eng" -p keyModel=INT -p maxSize=16384 -p writerThreads="$wt" \
      -t 8 "${W_FLAGS[@]}" -f 1
    WT_SCORE["${eng}_${wt}"]="$(score_of <"$f")"
    WT_EVICT["${eng}_${wt}"]="$(field_of "$f" evictions)"
  done
  {
    printf "| %s | ops/ms |" "$eng"
    for wt in $WRITERS; do printf " %s |" "${WT_SCORE[${eng}_${wt}]:--}"; done
    echo
    printf "| %s | evictions |" "$eng"
    for wt in $WRITERS; do printf " %s |" "${WT_EVICT[${eng}_${wt}]:--}"; done
    echo
  } >> "$REPORT"
done

echo "== 4b) 旗舰 putCycling（1GB 驻留, LOCK_FREE, wt=4, -t 8） =="
{
  echo "**旗舰 putCycling（WriteEvictionBenchmark.putCycling · LOCK_FREE · maxSize=${MX} · keySpace=${KS} · wt=4 · -t 8）**"
  echo
  echo "| 指标 | 值 |"
  echo "|---|---|"
} >> "$REPORT"
f="$RUN_DIR/pc_flagship.log"
jrun "$f" io.lcache.benchmark.WriteEvictionBenchmark.putCycling \
  -p engine=LOCK_FREE -p keyModel=INT -p maxSize="$MX" -p keySpace="$KS" -p writerThreads=4 \
  -t 8 "${W_FLAGS[@]}" -f 1
{
  printf "| ops/ms | %s |\n" "$(score_of <"$f")"
  printf "| evictions | %s |\n" "$(field_of "$f" evictions)"
  echo
} >> "$REPORT"

# ── 5) TTL 过期 + 后台清扫压力（小驻留：清扫 O(驻留) 全链扫描） ─────────────────
echo "== 5) TTL 过期 + 后台清扫压力（putUpdates, maxSize=16384, ttlUs 0 vs 1000） =="
{
  echo "## TTL 过期 + 后台清扫（TtlExpirySweepBenchmark.putUpdates, maxSize=16384, -t 8）"
  echo
  echo "> 驻留保持小规模：过期清扫是 O(驻留) 全链扫描（时间闸 1ms 摊销），故用小驻留隔离清扫代价、避免写线程饥饿。"
  echo
  echo "| 引擎 | 指标 | ttl=0 | ttl=1000µs |"
  echo "|---|---|---|---|"
} >> "$REPORT"

declare -A TTL_SCORE TTL_EXPIRES
TTLS="0 1000"
for eng in LOCK_FREE SYNCHRONIZED; do
  for ttl in $TTLS; do
    printf "  [%s ttl=%sµs] ...\n" "$eng" "$ttl" >&2
    f="$RUN_DIR/ttl_${eng}_${ttl}.log"
    jrun "$f" io.lcache.benchmark.TtlExpirySweepBenchmark.putUpdates \
      -p engine="$eng" -p maxSize=16384 -p ttlUs="$ttl" \
      -t 8 "${W_FLAGS[@]}" -f 1
    TTL_SCORE["${eng}_${ttl}"]="$(score_of <"$f")"
    TTL_EXPIRES["${eng}_${ttl}"]="$(field_of "$f" expires)"
  done
  {
    printf "| %s | ops/ms |" "$eng"
    for ttl in $TTLS; do printf " %s |" "${TTL_SCORE[${eng}_${ttl}]:--}"; done
    echo
    printf "| %s | expires |" "$eng"
    for ttl in $TTLS; do printf " %s |" "${TTL_EXPIRES[${eng}_${ttl}]:--}"; done
    echo
  } >> "$REPORT"
done

# ── 6) 读穿单飞去重 ────────────────────────────────────────────────────────
echo "== 6) 读穿单飞去重（keySpace × loader 延迟，-t 16） =="
{
  echo "## 读穿单飞去重（SingleFlightLoadBenchmark.invalidateThenLoad, -t 16）"
  echo
  echo "> dedup = 1 − loads/misses；越高说明 leader/follower 合并越有效（越多人等了同一次 loader）。"
  echo
  echo "| keySpace | loader延迟 | ops/ms | loads | misses | dedup |"
  echo "|---|---|---|---|---|---|"
} >> "$REPORT"

for ks in 16 256; do
  for dly in 0 300; do
    printf "  [keySpace=%s delay=%sµs] ...\n" "$ks" "$dly" >&2
    f="$RUN_DIR/sf_${ks}_${dly}.log"
    jrun "$f" io.lcache.benchmark.SingleFlightLoadBenchmark.invalidateThenLoad \
      -p keySpace="$ks" -p delayUs="$dly" \
      -t 16 "${W_FLAGS[@]}" -f 1
    score="$(score_of <"$f")"
    loads="$(field_of "$f" loads)"
    misses="$(field_of "$f" misses)"
    dedup="$(field_of "$f" dedup)"
    {
      printf "| %s | %sµs | %s | %s | %s | %s |\n" \
        "$ks" "$dly" "${score:--}" "${loads:--}" "${misses:--}" "${dedup:--}"
    } >> "$REPORT"
  done
done

# ── 7) 引擎微基准（EngineThroughputBenchmark，-t 4） ─────────────────────────
echo "== 7) 引擎微基准（EngineThroughputBenchmark, -t 4） =="
{
  echo "## 引擎微基准（EngineThroughputBenchmark, -t 4）"
  echo
  echo "> 键空间 2^13 微基准（数据量无关，测引擎单点）；单位 ops/ms。"
  echo
  echo "| 引擎 | 方法 | ops/ms |"
  echo "|---|---|---|"
} >> "$REPORT"
for eng in LOCK_FREE SYNCHRONIZED; do
  for m in getHit getMiss putUpdate mixedReadWrite; do
    printf "  [%s %s] ...\n" "$eng" "$m" >&2
    f="$RUN_DIR/eng_${eng}_${m}.log"
    jrun "$f" io.lcache.benchmark.EngineThroughputBenchmark.$m \
      -p engine="$eng" -t 4 "${W_FLAGS[@]}" -f 1
    {
      printf "| %s | %s | %s |\n" "$eng" "$m" "$(score_of <"$f")"
    } >> "$REPORT"
  done
done

# ── 8) size() 吞吐（CacheSizeThroughputBenchmark，ops/s，LOCK_FREE） ──────────
echo "== 8) size() 吞吐（CacheSizeThroughputBenchmark, ops/s, engine=LOCK_FREE） =="
{
  echo "## size() 吞吐（CacheSizeThroughputBenchmark, ops/s, engine=LOCK_FREE）"
  echo
  echo "> ① 规模无关：size() 只把 64 个 per-thread 计数行相加；② 叠加 size 的开销由 withSize=ON 的"
  echo "> 后台 size 线程窃取吞吐量化；③ 线程扩展：size() wait-free、随 -t 近线性。"
  echo
} >> "$REPORT"

# ① size() vs 规模（1M / 10M 驻留，31 写线程 + 1 size 线程）
{
  echo "**① size() 吞吐 vs 数据规模（-t 1，31 后台写线程）**"
  echo
  echo "| 数据规模 | ops/s |"
  echo "|---|---|"
} >> "$REPORT"
for s in $SZ_SCALES; do
  printf "  [size scale=%s] ...\n" "$s" >&2
  f="$RUN_DIR/sz_${s}.log"
  jrun "$f" io.lcache.benchmark.CacheSizeThroughputBenchmark.size \
    -p engine=LOCK_FREE -p scale="$s" -t 1 "${W_FLAGS[@]}" -f 1
  {
    printf "| %s | %s |\n" "$s" "$(score_of_s <"$f")"
  } >> "$REPORT"
done
echo >> "$REPORT"

# ② 叠加 size() 开销（readPct × withSize）
{
  echo "**② 叠加 size() 对 put/get 总吞吐的开销（-t 16）**"
  echo
  echo "| 读占比 | withSize | ops/s |"
  echo "|---|---|---|"
} >> "$REPORT"
for rp in 95 50; do
  for ws in OFF ON; do
    printf "  [mixed readPct=%s withSize=%s] ...\n" "$rp" "$ws" >&2
    f="$RUN_DIR/mx_${rp}_${ws}.log"
    jrun "$f" io.lcache.benchmark.CacheSizeThroughputBenchmark.mixed \
      -p engine=LOCK_FREE -p readPct="$rp" -p withSize="$ws" -t 16 "${W_FLAGS[@]}" -f 1
    {
      printf "| %s%% | %s | %s |\n" "$rp" "$ws" "$(score_of_s <"$f")"
    } >> "$REPORT"
  done
done
echo >> "$REPORT"

# ③ size() 线程扩展
{
  echo "**③ size() 线程扩展（scale=$SZ_SCALE，-t 扫）**"
  echo
  echo "| size 线程数 | 总 ops/s |"
  echo "|---|---|"
} >> "$REPORT"
for t in $SIZE_TS; do
  printf "  [size -t %s] ...\n" "$t" >&2
  f="$RUN_DIR/sz_t_${t}.log"
  jrun "$f" io.lcache.benchmark.CacheSizeThroughputBenchmark.size \
    -p engine=LOCK_FREE -p scale="$SZ_SCALE" -t "$t" "${W_FLAGS[@]}" -f 1
  {
    printf "| %s | %s |\n" "$t" "$(score_of_s <"$f")"
  } >> "$REPORT"
done

# ── 9) footprint 内存探测 ──────────────────────────────────────────────────
# 占 4g 堆较慢，CI/冒烟可用 FOOTPRINT=0 跳过；完整跑默认执行。
if [ "${FOOTPRINT:-1}" = "1" ]; then
  echo "== 9) footprint 内存探测（非 JMH；驻留 2^20/2^21，堆 4g） =="
  # footprint 自设 -Xms4g -Xmx4g；清掉上面的 JAVA_TOOL_OPTIONS 避免堆旗标叠加歧义
  env -u JAVA_TOOL_OPTIONS java -Xms4g -Xmx4g -XX:+UseSerialGC -XX:+AlwaysPreTouch \
    -cp "$JAR" io.lcache.benchmark.FootprintProbe | tee "$RUN_DIR/footprint.log"
fi

echo
echo "== 汇总报告已写入：$REPORT =="
sed -n '1,240p' "$REPORT"
echo "== done =="
