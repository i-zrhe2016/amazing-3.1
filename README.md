# Amazing3.1 Backtest & Optimization

基于 `Amazing3.1.mq4` 逻辑的 Python 回测与参数优化项目，当前主要针对 `USDCHF M5`。

## 1. 项目内容

- `Amazing3.1.mq4` / `Amazing3.1.mq4.bak`：原始 MT4 EA 文件
- `amazing31_python.py`：EA 逻辑的 Python 版本
- `backtest_usdchf.py`：USDCHF M5 回测引擎（含点差/滑点模型与数据下载）
- `optimize_usdchf_params.py`：1Y 随机搜索 + 3Y/5Y验证
- `optimize_usdchf_10y_pre_blowup.py`：10年严格不爆仓约束下最大利润优化
- `optimize_usdchf_10y_target100_no_blowup.py`：目标“每年 >=100%”并尽量不爆仓的优化器
- `download/`：历史数据缓存目录

## 2. 环境依赖

- Python 3.10+
- Node.js + npm（首次自动下载数据会用到 `npx dukascopy-node`）

建议在项目目录执行。

## 3. 快速开始

### 3.1 运行基础回测

```bash
python3 backtest_usdchf.py --years 1
```

说明：
- 自动读取/下载 `USDCHF M5 bid` 数据
- 输出净利润、回撤、胜率、PF、平均点差等

### 3.2 通用参数优化（1Y搜索）

```bash
python3 optimize_usdchf_params.py --trials 30 --seed 20260226
```

### 3.3 10年严格不爆仓 + 最大利润

```bash
python3 optimize_usdchf_10y_pre_blowup.py --trials 20 --seed 20260226
```

输出文件：
- `optimized_params_10y_no_blowup_max_profit.json`

### 3.4 每年目标 >=100% + 尽量不爆仓

```bash
python3 optimize_usdchf_10y_target100_no_blowup.py --trials 200 --seed 20260226
```

输出文件：
- `optimized_params_10y_target100_no_blowup.json`

`--trials` 越大，搜索更充分，但耗时更长。

## 4. 已有结果快照

基于当前仓库结果文件：

1. `optimized_params_10y_no_blowup_max_profit.json`
- 10年分窗 `blowup_years = 0`
- 总净利润 `sum_net_profit = 15417.53`
- 最差年份净利润 `min_year_net_profit = 826.90`

2. `optimized_params_10y_target100_no_blowup.json`（`--trials 200`）
- 10年分窗 `blowup_years = 0`
- 总净利润 `sum_net_profit = 18024.15`
- 每年 >=100% 达标年数 `pass_target_years = 0 / 10`
- 最差年份收益率 `min_year_return_pct = 8.57%`

## 5. 数据说明

- 主要策略脚本当前使用 `USDCHF M5`
- `download/` 下已有多个历史文件（含 merged 文件）
- 若目标 merged 文件不存在，脚本会自动分年下载并合并

## 6. 注意事项

- 回测为仿真模型，结果不代表实盘收益。
- “爆仓”在本项目中的判定：`equity <= 0` 或 `free_margin <= 0`。
- 优化结果依赖随机种子与搜索次数；建议用更高 `--trials` 做复验。

