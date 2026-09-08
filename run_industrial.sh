#!/usr/bin/env bash
# 工业形态 JMH 基准的运行文档/命令集合（HotReadZipfBenchmark / WriteEvictionBenchmark /
# FootprintProbe）。set -euo pipefail 会在 RejectedExecutionException 等失败时立刻中断，
# 便于发现"写密集下 -t 过高 / writerThreads 过小"的准入拒绝。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$ROOT/modules/lcache-benchmark/target/lcache-benchmarks.jar"

# 统一稳定旗标：单 fork、显式 warmup/measurement、显式 -t、-p 收敛矩阵。
# JMH 命令行旗标会覆盖类注解里的默认 @Warmup/@Measurement/@Fork。
W="-wi 5 -w 2s -i 5 -r 2s -f 1"
F="-foe true"   # 冒烟时打开，任何拒绝异常立刻炸出

echo "== 1) build =="
mvn -q -pl modules/lcache-benchmark -am package

echo "== 2) 读密集·单配置冒烟 + GC 分配率（-prof gc） =="
java -jar "$JAR" io.lcache.benchmark.HotReadZipfBenchmark.mixed95_5 \
  -p engine=LOCK_FREE -p keyModel=INT -p maxSize=16384 \
  -t 8 $W -f 1 -prof gc

echo "== 3) 读路径线程扫描（吞吐 vs 命中率随 -t 变化） =="
for t in 1 4 8 16; do
  echo "--- threads=$t ---"
  java -jar "$JAR" io.lcache.benchmark.HotReadZipfBenchmark.mixed95_5 \
    -p engine=LOCK_FREE,SYNCHRONIZED -p keyModel=STRING -p maxSize=4096,16384 \
    -t "$t" $W -f 1 2>&1 | grep -E "\[hot-read\]|thrpt" || true
done

echo "== 4) 写密集：writerThreads 吞吐上限扫描（保持 -t<=16） =="
java -jar "$JAR" io.lcache.benchmark.WriteEvictionBenchmark \
  -p engine=LOCK_FREE,SYNCHRONIZED -p keyModel=INT \
  -p maxSize=4096,16384 -p writerThreads=1,4,8 \
  -t 8 $W -f 1 2>&1 | tee "$ROOT/write_eviction.log" \
  | grep -E "\[write-evict\]|thrpt" || true
echo "（已存 write_eviction.log；校验 evictionCount ≈ putCycling 的 put 次数）"

echo "== 5) 无驱逐上限参照（maxSize=0，全量驻留≈100%命中） =="
java -jar "$JAR" io.lcache.benchmark.HotReadZipfBenchmark.mixed95_5 \
  -p engine=LOCK_FREE -p keyModel=INT -p maxSize=0 \
  -t 8 $W -f 1 2>&1 | grep -E "\[hot-read\]|thrpt" || true

echo "== 6) footprint 内存探测（非 JMH；建议按提示加大堆） =="
java -Xms2g -Xmx2g -XX:+UseSerialGC -XX:+AlwaysPreTouch \
  -cp "$JAR" io.lcache.benchmark.FootprintProbe

echo "== done =="
