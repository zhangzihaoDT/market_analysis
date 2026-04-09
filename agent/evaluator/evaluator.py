def evaluate(task: dict) -> dict:
    """
    评估工具执行结果，提取潜在问题。
    task 需包含以下字段：
      - query: 用户的原始问题
      - sql: 代理生成的 SQL（或 python 代码）
      - result_rows: 结果返回的行数
      - error: 是否有报错信息
    """
    issues = []

    # 1. 如果有明确的执行报错
    if task.get("error"):
        issues.append(f"Execution Error: {task['error']}")
    
    # 2. 如果结果集为空且没有报错（说明查询语法对，但没查出数据）
    if task.get("result_rows", 0) == 0 and not task.get("error"):
        issues.append("empty_result: 查询结果为空，可能过滤条件错误或关联失败")

    # 3. 如果生成的 SQL 存在低效/不精确的做法（比如 SELECT *）
    sql = str(task.get("sql", "")).upper()
    if "SELECT *" in sql:
        issues.append("low_precision: 查询不够精确，尽量避免直接使用 SELECT *")

    return {
        "success": len(issues) == 0,
        "issues": issues
    }
