# SGM Agentic - 智能数据分析 Agent

本项目是一个基于 Python 和大语言模型（如 DeepSeek）的智能数据分析 Agent。它能够自动从外部系统（Tableau）拉取业务数据、执行预设的指标计算脚本，并通过大模型自动编写、执行 DuckDB SQL 或 Python 代码来回答用户的复杂业务问题。

本项目创新性地引入了 **“Evaluator + Strategy Memory + Strategy Injection” 三层学习架构**，使得 Agent 具备了从历史错误中学习、总结经验并指导后续决策的能力。

## 项目结构
- `agent/`：Agent 核心认知架构
  - `evaluator/`：包含轻量级规则评估器，负责在每次 SQL 执行后判断任务质量并抛出 Issues（如发现 `SELECT *`、查询结果为空等）。
  - `memory/`：包含经验记忆模块（基于 DuckDB），分为 `strategy_store.py`（存储执行日志与经验模式）、`pattern_extractor.py`（基于 LLM 的经验总结器）和 `strategy_retriever.py`（经验检索器）。
- `data/`：存放拉取的基础数据（CSV）和数据源配置文件（`data_info.md`）。还会自动生成对应的 `.schema.json` 供大模型感知数据结构。
- `out/`：存放由指标脚本计算生成的派生数据表及 Schema。
- `scripts/`：存放业务数据处理和 KPI 计算的 Python 脚本。
- `tools/`：供 Agent 调用的工具集合，如提供 DuckDB SQL 查询能力的 `execute_sql.py`。
- `main.py`：Agent 的核心中枢，负责与大模型对话、规划任务、执行工具、评估结果、检索经验并返回最终分析结果。

## 1. 数据获取与处理 (`data/fetch.py`)

支持从 Tableau 拉取数据，自动进行长宽表转换，并自动为每个 CSV 生成对应的 `.schema.json` 文件供 Agent 动态感知。

### 基本使用
```bash
# 默认全量拉取 data_info.md 中配置的所有视图
python3 data/fetch.py

# 支持按需只拉取指定视图（极大节省时间）
python3 data/fetch.py --view-name 分价格段量价
python3 data/fetch.py --view-name 细分市场销量
python3 data/fetch.py --view-name 重点关注新能源品牌
```

### 3 个自动执行的步骤：
1. **数据获取**：从 `data_info.md` 配置的视图导出 CSV 到 `data/`。
2. **数据整理（转置聚合）**：若 CSV 含“度量名称”+“度量值”两列，则按其余维度分组并展开。**系统会自动对相同维度的数据进行求和聚合**，避免细粒度数据丢失。
3. **自动更新 Schema**：不仅会更新 `data_info.md`，还会在同目录下生成附带业务描述的 `xxx.schema.json` 文件。

### 可选参数：
- `--timeout 120`：调整请求超时秒数
- `--dry-run`：只打印将导出的目标，不实际请求/落盘
- `--no-fetch`：跳过拉取，直接对现有 CSV 做转置 + 更新 schema
- `--no-transform`：跳过转置聚合
- `--no-schema`：跳过 schema 更新

---

## 2. 品牌指标计算脚本 (`scripts/main_brand_sales_kpi.py`)

运行该脚本可从基础表计算同比、环比、累计等复杂业务指标。
```bash
python3 scripts/main_brand_sales_kpi.py
```
- 计算结果写入 `out/重点关注新能源品牌_品牌指标.csv`
- 自动生成同名 schema JSON：`out/重点关注新能源品牌_品牌指标.schema.json`

---

## 3. DeepSeek Agent 核心逻辑 (`main.py`)

这是项目的核心调度入口，采用单次 Prompt 统筹规划与多轮自我修复（Self-Correction）的执行闭环。

### 核心特性：
- **Evaluator 质量评估**：每次执行工具后，在后台轻量评估质量，捕捉逻辑缺陷或低效查询（如使用了 `SELECT *`）。
- **Strategy Memory (经验记忆库)**：每次运行的日志落盘在 `strategy_memory.db`，不再用完即弃。
- **Self-Learning (自动学习)**：通过 `python main.py --learn` 触发 LLM 反思过去失败的案例，抽取出 `pattern`、`strategy`、`anti_pattern` 存入知识库，形成持久记忆。
- **Strategy Injection (策略注入)**：每次用户提问前，检索最高度匹配的经验策略并动态注入 System Prompt，使 Agent 从“零思考”转变为“基于经验决策”。
- **Schema 动态感知**：启动时自动扫描 `data/` 和 `out/` 下的 `.schema.json`，新加表无需修改代码即可被 Agent 识别并使用。
- **DuckDB 极速查询**：内置 `execute_sql` 工具，大模型可以直接写 SQL（支持 JOIN、GROUP BY）在内存中极速查询 CSV，彻底告别写繁琐的 Python 处理代码。
- **动态 Python 执行与自我修复**：支持 `execute_python_code` 工具，大模型可动态编写并执行复杂 Python 脚本。如果执行报错，系统会将报错信息传回给模型，模型会自动修正代码后重试。

### 执行工作流（最多 10 轮循环）：
1. 接收问题，匹配历史经验策略（Strategy Injection）。
2. 优先思考并设计解决问题的逻辑（遵守避坑指南）。
3. 优先使用 `execute_sql` 或 `execute_python_code` 探索数据。
4. Evaluator 后台评估任务质量并落库。如果报错，自动分析错误并重试。
5. 提取到核心数据后强制终止，直接输出纯文本回答。

### 三种模式说明：
- `python3 main.py --query "..."`：普通对话模式
  - 速度快、消耗低，会自动读取记忆库优化查询策略，适合大多数日常问答和数据查询。
- `python3 main.py --learn`：学习模式
  - 让 Agent 反思过去执行不佳的查询，将“隐性经验”总结为“显性规则”固化到数据库中。
- `python3 main.py --thinking --query "..."`：思考模式（thinking enabled）
  - 模型会输出更强的内部推理过程，逻辑更“稳”，但速度较慢、token 消耗更高。

### 使用示例：
```bash
# 查询基础表（Agent 会自动写 SQL 过滤、汇总并返回）
python3 main.py --query "20万以上新能源市场 2026 年 2 月总销量"

# 跨表查询与计算
python3 main.py --query "运行 main_brand_sales_kpi.py，并找出 2026-02 月同比最高的品牌，给出对应的 max月销量/上月销量/月环比/去年同月销量"

# 使用思考模式深入分析
python3 main.py --thinking --query "2026年 2 月智己品牌的销量如何?"
```

**示例输出（节选）：**
```
最终答案：
20万以上新能源市场在2026年2月的总销量为 191,773 辆。
其中各价格段销量分布如下：
  - 20-25万: 66,178 辆 (占比 34.5%)
  - 25-30万: 53,166 辆 (占比 27.7%)
  - 30-35万: 36,860 辆 (占比 19.2%)
...
```
