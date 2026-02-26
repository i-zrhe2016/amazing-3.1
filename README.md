# Amazing3.1 Rust Optimizer

本仓库当前仅保留 Rust 参数优化流程。

## 1. 项目结构

- `Amazing3.1.mq4` / `Amazing3.1.mq4.bak`：原始 MT4 EA 文件
- `rust_optimizer/`：纯 Rust 参数优化器（数据读取、回测仿真、参数搜索）
- `download/`：历史数据目录（CSV）
- `optimized_params_audnzd_10y_dd80_rust.json`：Rust 优化结果示例

## 2. 依赖

- Rust（建议 stable）
- Cargo

## 3. 数据要求

优化器会从 `download/` 里选择 `merged` 数据文件。

推荐文件命名：

- `{symbol_lower}-m5-bid-{start_date}-{end_date}-merged.csv`
- 例如：`audnzd-m5-bid-2006-02-26-2026-02-26-merged.csv`

CSV 字段格式（含表头）：

- `timestamp,open,high,low,close`
- `timestamp` 为毫秒时间戳

## 4. 快速开始

### 4.1 直接运行

```bash
cargo run --manifest-path rust_optimizer/Cargo.toml -- \
  --symbol AUDNZD \
  --years 10 \
  --trials 3000 \
  --seed 20260226 \
  --drawdown-limit 80 \
  --out optimized_params_audnzd_10y_dd80_rust_t3000.json
```

### 4.2 Release 二进制（更快）

```bash
cargo build --manifest-path rust_optimizer/Cargo.toml --release
./rust_optimizer/target/release/rust_optimizer \
  --symbol AUDNZD \
  --years 10 \
  --trials 3000 \
  --seed 20260226 \
  --drawdown-limit 80 \
  --out optimized_params_audnzd_10y_dd80_rust_t3000.json
```

## 5. 参数说明

- `--symbol`：交易品种，默认 `AUDNZD`
- `--years`：回测年份窗口，默认 `10`
- `--trials`：搜索次数，默认 `120`，必须 `>= 1`
- `--seed`：随机种子，默认 `20260226`
- `--drawdown-limit`：年度最大回撤上限（百分比），默认 `80`
- `--out`：输出 JSON 文件路径
- `--data-file`：可选，手动指定 CSV 文件（跳过自动选择）

## 6. 输出结果

输出 JSON 包含：

- `best_feasible`：满足回撤约束的最优参数（若存在）
- `best_any`：不考虑约束时全局最优参数
- `selected_result`：最终选中的参数集
- `chosen_boundaries`：自适应边界收缩结果
- `yearly_results`：逐年回测指标

## 7. 说明

- 目标函数：在 `worst_year_max_drawdown_pct < drawdown_limit` 约束下最大化利润。
- 算法：`adaptive elite search + boundary refinement`。
- 当前仅优化 3 个参数：`step`、`lot`、`k_lot`；其余参数固定为 `Amazing3.1.mq4.bak` 默认值。
- 回测与优化结果受数据质量、随机种子与 `--trials` 影响。
