%md

# 📘 Modular Execution Patterns in Databricks

This guide explains how to modularize Databricks pipelines using `%run` and `dbutils.notebook.run()` effectively. It includes usage patterns, best practices, and pros/cons of each approach.

----

## 1️⃣ `%run` – Inline Execution

- **Purpose**: Share functions, variables, and constants across notebooks.
- **Usage**:
```python
# Must be in its own cell
%run /Repos/your_user@databricks.com/databricks-pipelines/pipeline1_batch_delta/utils/write_utils
```

- **Behavior**: Injects the code from the referenced notebook inline.
- ✅ **Pros**:
  - Simple
  - Shares functions and variables
- ❌ **Cons**:
  - No parameter passing
  - No return values
  - Sequential execution only

---

## 2️⃣ `dbutils.notebook.run()` – Isolated Execution

- **Purpose**: Run notebooks programmatically with arguments and capture outputs.
- **Usage**:
```python
result = dbutils.notebook.run("/path/to/notebook", timeout_seconds=300, arguments={"param": "value"})
```

- ✅ **Pros**:
  - Supports arguments
  - Returns a string
  - Useful in workflows
- ❌ **Cons**:
  - Isolated environment
  - No variable sharing
  - Return values must be serialized strings

---

## 3️⃣ Best Practices

| Use Case | Recommended Approach |
|----------|-----------------------|
| Utility functions, constants | `%run` |
| Job workflows or pipelines | `dbutils.notebook.run()` |
| Parallel execution | `dbutils.notebook.run()` |
| Reusing logic across bronze/silver/gold | `%run` for logic, `dbutils.notebook.run()` for orchestration |

---

## 🧩 Example from This Project

### In `mock_finance_invoices.py`:
```python
%run /Repos/your_user@databricks.com/databricks-pipelines/pipeline1_batch_delta/utils/write_utils
```

- Loads helper functions
- Keeps notebook logic clean and modular

---

