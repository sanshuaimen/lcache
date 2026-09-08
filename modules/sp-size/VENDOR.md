# sp-size：嵌入式 SP 无锁 size 算法（vendor 子集）

本模块是从研究仓库裁剪出来的**纯 vendor 源码**，作为 `lcache-core` 的无锁数据面，
**未做任何逻辑修改**（仅抽取文件、保留包名与版权/GPL v3 头）。

## 来源与裁剪

- 上游：SP（Concurrent Size，Gal Sela & Erez Petrank，"Concurrent Size"，PODC'22）
  开源实现的**哈希表 + per-thread 计数**子集。完整研究仓库已归档在仓库根目录 `_archive/sp-core`。
- 保留文件：
  - `algorithms/size/SizeHashTable.java` —— 无锁链式哈希表 + 内联 SP size 计数（数据面引擎）
  - `algorithms/size/core/` —— `SizeCalculator`（per-thread 计数）、`UpdateOperations`、
    `UpdateInfo`、`UpdateInfoHolder`、`Backoff`
  - `measurements/support/ThreadID.java` —— per-thread 槽位分配（`MAX_THREADS = 64`）
- 裁剪掉：BST / 跳表 / VCAS / 迭代器 / 测量框架等与"无锁哈希数据面"无关的部分。
  原 `_archive/sp-core/algorithms/vcas/Camera.java` 使用了 `@Contended`，裁剪后本模块
  **编译/运行均不再需要 `--add-exports`**。

## 使用条件（lcache-core 依赖这些约束，见根 README"边界与使用条件"）

1. 写路径（`put/remove`）必须在**已注册 ThreadID 槽位的写线程**上执行 → 缓存层收口到
   线程亲和写池；
2. 读路径（`get/size`）无需注册、无锁；
3. key 须可排序（`Comparable` 或构造传入 `Comparator`）；
4. 哈希表**定容不可扩容** → 缓存层在构建时确定容量。

## 许可

各源文件保留上游 GPL v3 版权头与许可说明。
