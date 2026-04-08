```
python3 data/fetch.py
```

## 3 个步骤（默认全自动执行）：

- step1 数据获取 ：从 data_info.md 的 3 个视图导出 CSV 到 data/ （原逻辑保留）
- step2 数据整理（转置） ：若 CSV 含 度量名称 + 度量值 两列，则按除这两列外的字段分组，把 度量名称 的不同取值展开为多列（原 度量值 写入对应列），并 原地覆盖 原 CSV
  - 示例： 细分市场销量.csv 已变为 … ,24 年销量,25 年销量,… 这种宽表结构
- step3 自动更新 schema ：基于整理后的 CSV 重新计算并覆盖更新 data_info.md 的 ## 信息模块（Schema） 章节（字段｜举例｜dtype｜空值率）

## 可选参数：

- --timeout 120 调整请求超时秒数
- --dry-run 只打印将导出的目标，不实际请求/落盘

### 可选参数（便于单独跑 step2/step3）：

- --no-fetch ：跳过拉取，直接对现有 CSV 做转置 + 更新 schema
- --no-transform ：跳过转置
- --no-schema ：跳过 schema 更新

---

```
python3 scripts/brand_sales_kpi.py
```

- 继续将品牌指标结果写到 out/重点关注新能源品牌\_品牌指标.csv
- 新增自动生成同名 schema JSON： out/重点关注新能源品牌\_品牌指标.schema.json

---

## DeepSeek Agent（main.py）

- step1（planning） ：每轮先让模型输出本轮 planning（不调用工具），要求明确：

- 使用基础表还是派生表：基础表位于 ./data（细分市场销量/分价格段量价/重点关注新能源品牌），派生表位于 ./out
- 需要跑哪些脚本（如需要）以生成派生表（位于 ./scripts）
- 需要读取哪些 schema JSON 来确定列含义
- 需要对哪些 CSV 做哪些 query_csv 查询（filters/select/order_by/limit）

- step2（执行查询） ：按 planning 进入工具执行阶段：

- 数据源选择策略：
  - 明细核对/字段级问题/明确指向基础表 → 优先查询 ./data
  - 指标汇总/同比环比/排名/品牌级 KPI → 优先 run_script 生成 ./out，再查询 ./out
  - 两者都需 → 先判断派生表是否必要，必要时先 run_script，再用基础表补充
- 执行优先级与约束：
  - 优先“写代码一次算清楚”：返回一段 Python 代码，读取 ./data 或 ./out 下的 CSV，完成筛选/聚合并打印最终答案与关键中间汇总
  - 代码需使用定界符包裹，主程序会自动执行：
    - ‹execute_python›
    - …Python 代码…
    - ‹/execute_python›
  - 如确需派生表，最多执行一次 run_script；随后用代码直接对 CSV 计算
  - 使用 query_csv 时 path 必须是 CSV 文件，不能是目录；否则工具会返回可选 CSV 列表
  - 主程序会在终端 stderr 输出每轮 planning/执行日志与工具调用摘要，stdout 输出最终答案
  - 信息不足时输出 NEED_MORE 进入下一轮；否则输出最终答案并退出

### 两种模式有什么不同？

- `python3 main.py --query "..."`：普通对话模式（非思考模式）
  - 更快、输出更短，适合大多数日常问答/数据查询
  - 支持工具调用与自动执行 ‹execute_python› 代码块，按需循环（最多 5 轮）后给出最终回答

- `python3 main.py --thinking --query "..."`：思考模式（thinking enabled）
  - 模型会输出更强的推理过程（内部会携带 reasoning_content），通常更“稳”，但更慢、token 消耗更高
  - 同样支持工具调用与自动执行 ‹execute_python› 代码块，按需循环（最多 5 轮）
  - 注意：思考模式下部分采样参数（如 temperature/top_p 等）不会生效（兼容性考虑仍可传，但会被忽略）

### 使用示例

```bash
python3 main.py --query "运行 main_brand_sales_kpi.py，并找出 2026-02 月同比最高的品牌，给出对应的 max月销量/上月销量/月环比/去年同月销量"
```

```bash
python3 main.py --thinking --query "2026年 2 月智己品牌的销量如何?"
```

示例输出（节选）：

```
2026年2月智己品牌的销量为 3,136 辆
- 上月销量（2026年1月）：4,934 辆
- 月环比：-36.44%
- 去年同月销量（2025年2月）：2,543 辆
- 月同比：+23.32%
- 今年累计销量（1-2月）：8,070 辆
- 去年累计销量（1-2月）：6,145 辆
- 累计同比：+31.33%
```
