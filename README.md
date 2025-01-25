# **Neuro-DB: Simplifying Multi-Database Interactions in Python**  

**Neuro-DB** is a powerful orchestration layer that bridges the gap between developers and databases. It provides an intuitive, unified interface to manage multiple database connections effortlessly, with seamless **Pandas** integration for querying, inserting, and updating records across multiple database types, including **PostgreSQL, MySQL, SQL Server, and SQLite.**

---

## **Key Features**  

- 🔗 **Multi-Database Connectivity** – Supports PostgreSQL, MySQL, SQL Server, and SQLite.  
- ⚡ **Simplified Operations** – Intuitive methods like `get_df`, `upsert_df`, and `execute_query`.  
- 🐼 **Seamless Pandas Integration** – Read from and write directly to databases using Pandas DataFrames.  
- 🔒 **Secure Connection Management** – Environment-based credential handling for enhanced security.  
- 📈 **Logging & Performance Tracking** – Monitor and optimize query execution.  
- ⚙️ **Async and Sync Support** – Handle operations efficiently with async execution.  
- 🛠 **Minimal Setup Required** – Easy-to-use interface with powerful functionality.

---

## **Installation**  

Install Neuro-DB via pip:  

```bash
pip install neuro-db
```

---

## **Quick Start**  

### **1. Configure your databases**  

```python
from neurodb import DatabaseManager

db_configs = {
    "postgres_main": {
        "dialect": "postgresql",
        "user": "admin",
        "password": "securepass",
        "host": "localhost",
        "port": 5432,
        "database": "company_db"
    }
}

db = DatabaseManager(db_configs)
```

---

### **2. Perform database operations**  

#### **Fetch data as a Pandas DataFrame**  
```python
df = db.get_df("postgres_main", "SELECT * FROM employees WHERE department = %s", params=("HR",))
print(df.head())
```

#### **Upsert data from a Pandas DataFrame**  
```python
import pandas as pd

data = pd.DataFrame([
    {"id": 1, "name": "Alice", "department": "HR"},
    {"id": 2, "name": "Bob", "department": "IT"}
])

db.upsert_df("postgres_main", "employees", data, unique_key="id")
```

#### **Write DataFrame directly to a table**  
```python
df.to_sql("employees_backup", db.get_connection("postgres_main"), if_exists="replace", index=False)
```

#### **Execute raw SQL queries**  
```python
result = db.execute_query("postgres_main", "UPDATE employees SET status = %s WHERE id = %s", params=("active", 1))
```

---

## **Supported Databases**  

Neuro-DB supports the following databases out of the box:  

- **PostgreSQL** (via `psycopg2`)  
- **MySQL/MariaDB** (via `pymysql`)  
- **SQL Server** (via `pyodbc`)  
- **SQLite** (built-in Python module)

---

## **Configuration Options**  

You can configure Neuro-DB via environment variables or a YAML/JSON configuration file.  
Example YAML config:

```yaml
databases:
  mysql_db:
    dialect: mysql
    user: root
    password: password123
    host: localhost
    port: 3306
    database: sales_db

  sqlite_db:
    dialect: sqlite
    database: my_local.db
```

---

## **Why Choose Neuro-DB?**  

- **Unified API Across Databases** – Work consistently across different database systems.  
- **Boost Productivity** – Focus on building applications without database complexity.  
- **Optimized Performance** – Built-in query optimizations and best practices.  
- **Production-Ready** – With logging, retries, and error handling baked in.  

---

## **Contributing**  

We welcome contributions! To contribute:  

1. Fork the repository.  
2. Create a feature branch.  
3. Submit a pull request.  

---

## **License**  

This project is licensed under the **Apache 2.0 License** – free to use with attribution.

---

