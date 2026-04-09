# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Build Genie Space
# MAGIC
# MAGIC This notebook creates and configures a Genie Space for querying the structured Delta tables.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run `01_create_delta_tables.py` (creates tables with semantic comments)
# MAGIC - A SQL Pro or Serverless SQL Warehouse must be available
# MAGIC
# MAGIC **Reference:** https://docs.databricks.com/aws/en/genie/set-up

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-import config after restart
CATALOG = "ka_genie_demo"
SCHEMA = "ecommerce"
SQL_WAREHOUSE_ID = "<your-sql-warehouse-id>"

GENIE_SPACE_NAME = "E-Commerce Analytics"
GENIE_SPACE_DESCRIPTION = (
    "Query structured e-commerce data including customers, products, orders, "
    "and support tickets. Use this space to answer questions about sales metrics, "
    "customer behavior, order trends, and support analytics."
)

CUSTOMERS_TABLE = f"{CATALOG}.{SCHEMA}.customers"
PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.products"
ORDERS_TABLE = f"{CATALOG}.{SCHEMA}.orders"
ORDER_ITEMS_TABLE = f"{CATALOG}.{SCHEMA}.order_items"
SUPPORT_TICKETS_TABLE = f"{CATALOG}.{SCHEMA}.support_tickets"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A: Create Genie Space via REST API
# MAGIC
# MAGIC The Genie Space can be created programmatically using the Databricks REST API.

# COMMAND ----------

import requests
import json

workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Discover available SQL Warehouses

# COMMAND ----------

# List SQL warehouses to find a suitable one
warehouses_resp = requests.get(
    f"{workspace_url}/api/2.0/sql/warehouses",
    headers=headers,
)
warehouses_resp.raise_for_status()
warehouses = warehouses_resp.json().get("warehouses", [])

print("Available SQL Warehouses:")
print("-" * 80)
for wh in warehouses:
    status = wh.get("state", "UNKNOWN")
    wh_type = wh.get("warehouse_type", "UNKNOWN")
    print(f"  ID: {wh['id']}  |  Name: {wh['name']}  |  Type: {wh_type}  |  Status: {status}")

# Auto-select the first running serverless or pro warehouse
selected_warehouse_id = None
for wh in warehouses:
    if wh.get("warehouse_type") in ("PRO", "SERVERLESS"):
        selected_warehouse_id = wh["id"]
        print(f"\nAuto-selected warehouse: {wh['name']} ({wh['id']})")
        break

if not selected_warehouse_id:
    if warehouses:
        selected_warehouse_id = warehouses[0]["id"]
        print(f"\nUsing first available warehouse: {warehouses[0]['name']}")
    else:
        print("\nERROR: No SQL warehouses found. Please create one first.")

SQL_WAREHOUSE_ID = selected_warehouse_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create the Genie Space

# COMMAND ----------

# Create the Genie Space via Databricks SDK
# The API requires serialized_space with tables sorted alphabetically by identifier
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

table_identifiers = sorted([
    CUSTOMERS_TABLE,
    PRODUCTS_TABLE,
    ORDERS_TABLE,
    ORDER_ITEMS_TABLE,
    SUPPORT_TICKETS_TABLE,
])

serialized_space = json.dumps({
    "version": 2,
    "data_sources": {
        "tables": [{"identifier": t} for t in table_identifiers]
    },
    "sample_questions": [
        {"id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4", "question": ["Who are the top 5 customers by lifetime value?"]},
        {"id": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5", "question": ["What is the total revenue by product category?"]},
        {"id": "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6", "question": ["How many support tickets are still open?"]},
    ],
    "instructions": {
        "text_instructions": (
            "TechCommerce is an e-commerce company. "
            "Customers have membership tiers: Bronze, Silver, Gold, Platinum (based on annual spending). "
            "Orders have statuses: Processing, Shipped, Delivered, Returned, Cancelled. "
            "Revenue should only count Delivered orders unless otherwise specified. "
            "Product margin = price - cost. "
            "When asked about top/best customers, order by lifetime_value. "
            "When asked about popular products, order by units sold. "
            "Table joins: customers.customer_id -> orders.customer_id, "
            "orders.order_id -> order_items.order_id, "
            "products.product_id -> order_items.product_id, "
            "customers.customer_id -> support_tickets.customer_id."
        )
    }
})

space_id = None

# First check if a space with this name already exists
print(f"Checking for existing Genie Space named '{GENIE_SPACE_NAME}'...")
try:
    existing_spaces = list(w.genie.list_spaces())
    for s in existing_spaces:
        if s.title and GENIE_SPACE_NAME.lower() in s.title.lower():
            space_id = s.space_id
            print(f"Found existing space: {s.title} (ID: {space_id})")
            break
except Exception as e:
    print(f"Could not list spaces: {e}")

if not space_id:
    print("Creating new Genie Space...")
    try:
        result = w.genie.create_space(
            warehouse_id=SQL_WAREHOUSE_ID,
            serialized_space=serialized_space,
            title=GENIE_SPACE_NAME,
            description=GENIE_SPACE_DESCRIPTION,
        )
        space_id = result.space_id
        print(f"Genie Space created successfully!")
        print(f"  Space ID: {space_id}")
        print(f"  Title: {result.title}")
        print(f"  Warehouse: {SQL_WAREHOUSE_ID}")
        print(f"  Tables: {len(table_identifiers)}")
    except Exception as e:
        print(f"Error creating Genie Space: {e}")
        space_id = "PLACEHOLDER_SET_MANUALLY"
        print("Please create the Genie Space via UI and set space_id manually.")

print(f"\nFinal space_id = {space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Add Example SQL Queries (Instructions)
# MAGIC
# MAGIC Example queries help Genie understand the SQL patterns and business logic
# MAGIC for your data. These serve as few-shot examples.

# COMMAND ----------

# Example SQL queries to add as instructions
example_queries = [
    {
        "title": "Top customers by lifetime value",
        "query": f"""SELECT customer_id, first_name, last_name, membership_tier, lifetime_value
FROM {CUSTOMERS_TABLE}
ORDER BY lifetime_value DESC
LIMIT 10"""
    },
    {
        "title": "Monthly revenue trend for delivered orders",
        "query": f"""SELECT DATE_TRUNC('month', order_date) AS month,
       COUNT(*) AS order_count,
       ROUND(SUM(total_amount), 2) AS revenue
FROM {ORDERS_TABLE}
WHERE status = 'Delivered'
GROUP BY 1
ORDER BY 1"""
    },
    {
        "title": "Best-selling products by revenue",
        "query": f"""SELECT p.product_name, p.category,
       COUNT(oi.item_id) AS units_sold,
       ROUND(SUM(oi.line_total), 2) AS total_revenue,
       ROUND(SUM(oi.line_total) - SUM(oi.quantity * p.cost), 2) AS total_profit
FROM {ORDER_ITEMS_TABLE} oi
JOIN {PRODUCTS_TABLE} p ON oi.product_id = p.product_id
GROUP BY 1, 2
ORDER BY total_revenue DESC"""
    },
    {
        "title": "Support ticket metrics by category",
        "query": f"""SELECT category,
       COUNT(*) AS total_tickets,
       SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END) AS resolved,
       SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
       ROUND(AVG(satisfaction_score), 1) AS avg_satisfaction,
       ROUND(AVG(DATEDIFF(resolved_date, created_date)), 1) AS avg_resolution_days
FROM {SUPPORT_TICKETS_TABLE}
GROUP BY 1
ORDER BY total_tickets DESC"""
    },
    {
        "title": "Customer order history with product details",
        "query": f"""SELECT c.first_name || ' ' || c.last_name AS customer_name,
       c.membership_tier,
       o.order_id, o.order_date, o.status, o.total_amount,
       p.product_name, oi.quantity, oi.line_total
FROM {ORDERS_TABLE} o
JOIN {CUSTOMERS_TABLE} c ON o.customer_id = c.customer_id
JOIN {ORDER_ITEMS_TABLE} oi ON o.order_id = oi.order_id
JOIN {PRODUCTS_TABLE} p ON oi.product_id = p.product_id
WHERE c.customer_id = :customer_id
ORDER BY o.order_date DESC"""
    },
    {
        "title": "Revenue by customer membership tier",
        "query": f"""SELECT c.membership_tier,
       COUNT(DISTINCT c.customer_id) AS customer_count,
       COUNT(DISTINCT o.order_id) AS order_count,
       ROUND(SUM(o.total_amount), 2) AS total_revenue,
       ROUND(AVG(o.total_amount), 2) AS avg_order_value
FROM {CUSTOMERS_TABLE} c
JOIN {ORDERS_TABLE} o ON c.customer_id = o.customer_id
WHERE o.status = 'Delivered'
GROUP BY 1
ORDER BY total_revenue DESC"""
    },
]

# Note: Example queries are typically added via the UI's Configure > Instructions tab.
# We'll print them here for manual addition or API usage.
print("=" * 80)
print("EXAMPLE SQL QUERIES FOR GENIE INSTRUCTIONS")
print("Add these in: Genie Space > Configure > Instructions > SQL Queries tab")
print("=" * 80)

for i, eq in enumerate(example_queries, 1):
    print(f"\n--- Example {i}: {eq['title']} ---")
    print(eq["query"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Add General Instructions (Text-based)

# COMMAND ----------

general_instructions = """
Business Context:
- TechCommerce is an e-commerce company selling electronics, furniture, and accessories
- Customers have membership tiers: Bronze, Silver, Gold, Platinum (based on annual spending)
- Orders can have statuses: Processing, Shipped, Delivered, Returned, Cancelled
- Support tickets are categorized: Return, Warranty, Shipping, Product Inquiry, Order, General

Key Business Logic:
- Revenue should only count 'Delivered' orders unless otherwise specified
- Lifetime value in the customers table represents total historical spending
- Product margin = price - cost
- When asked about "top" or "best" customers, default to ordering by lifetime_value
- When asked about "popular" products, default to ordering by units sold (from order_items)
- Discount percentage in orders is applied to the pre-discount total
- Support ticket satisfaction scores range from 1-5 (5 is best); NULL means unresolved

Table Relationships:
- customers.customer_id -> orders.customer_id (one customer, many orders)
- orders.order_id -> order_items.order_id (one order, many items)
- products.product_id -> order_items.product_id (one product in many order items)
- customers.customer_id -> support_tickets.customer_id
- orders.order_id -> support_tickets.order_id (optional)
- products.product_id -> support_tickets.product_id (optional)
"""

print("=" * 80)
print("GENERAL INSTRUCTIONS FOR GENIE")
print("Add these in: Genie Space > Configure > Instructions > Text tab")
print("=" * 80)
print(general_instructions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Test the Genie Space via Conversation API

# COMMAND ----------

import time

def query_genie(question: str, space_id: str, max_wait: int = 120) -> dict:
    """
    Send a question to the Genie Space and wait for a response.

    Returns a dict with keys: 'status', 'text', 'sql', 'data'
    """
    # Start a new conversation
    start_resp = requests.post(
        f"{workspace_url}/api/2.0/genie/spaces/{space_id}/start-conversation",
        headers=headers,
        json={"content": question},
    )
    start_resp.raise_for_status()
    start_data = start_resp.json()

    conversation_id = start_data["conversation"]["id"]
    message_id = start_data["message"]["id"]

    # Poll for completion
    poll_interval = 2
    start_time = time.time()

    while time.time() - start_time < max_wait:
        msg_resp = requests.get(
            f"{workspace_url}/api/2.0/genie/spaces/{space_id}/"
            f"conversations/{conversation_id}/messages/{message_id}",
            headers=headers,
        )
        msg_resp.raise_for_status()
        message = msg_resp.json()
        status = message.get("status", "UNKNOWN")

        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            break

        time.sleep(poll_interval)
        poll_interval = min(poll_interval * 1.5, 10)

    result = {
        "status": status,
        "text": None,
        "sql": None,
        "data": None,
    }

    if status == "COMPLETED":
        attachments = message.get("attachments", [])
        for att in attachments:
            if att.get("text"):
                result["text"] = att["text"].get("content", "")
            if att.get("query"):
                result["sql"] = att["query"].get("query", "")
                # Fetch query results
                att_id = att["attachment_id"]
                try:
                    data_resp = requests.get(
                        f"{workspace_url}/api/2.0/genie/spaces/{space_id}/"
                        f"conversations/{conversation_id}/messages/{message_id}/"
                        f"attachments/{att_id}/query-result",
                        headers=headers,
                    )
                    if data_resp.status_code == 200:
                        result["data"] = data_resp.json()
                except Exception:
                    pass

    return result


# Test queries
test_questions = [
    "Who are the top 5 customers by lifetime value?",
    "What is the total revenue by product category?",
    "How many support tickets are still open?",
    "What is the average order value by membership tier?",
    "Which products have been returned?",
]

print("=" * 80)
print("GENIE SPACE TEST QUERIES")
print("=" * 80)

if space_id and space_id != "PLACEHOLDER_SET_MANUALLY":
    for i, q in enumerate(test_questions, 1):
        print(f"\n--- Test {i} ---")
        print(f"Q: {q}")
        try:
            result = query_genie(q, space_id)
            print(f"Status: {result['status']}")
            if result["sql"]:
                print(f"SQL: {result['sql'][:200]}...")
            if result["text"]:
                print(f"Text: {result['text'][:200]}...")
            if result["data"]:
                rows = result["data"].get("data_array", [])
                print(f"Rows returned: {len(rows)}")
        except Exception as e:
            print(f"Error: {e}")
else:
    print("\nSkipping test queries -- space_id is not set.")
    print("Create the Genie Space via UI and re-run this section with a valid space_id.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option B: Create Genie Space via UI
# MAGIC
# MAGIC If you prefer to set up the Genie Space through the Databricks UI:
# MAGIC
# MAGIC 1. Navigate to **Genie** in the left sidebar
# MAGIC 2. Click **New** to create a new Genie space
# MAGIC 3. **Name:** E-Commerce Analytics
# MAGIC 4. **Select SQL Warehouse:** Choose a Pro or Serverless warehouse
# MAGIC 5. **Add Tables:**
# MAGIC    - `ka_genie_demo.ecommerce.customers`
# MAGIC    - `ka_genie_demo.ecommerce.products`
# MAGIC    - `ka_genie_demo.ecommerce.orders`
# MAGIC    - `ka_genie_demo.ecommerce.order_items`
# MAGIC    - `ka_genie_demo.ecommerce.support_tickets`
# MAGIC 6. Go to **Configure > Instructions**:
# MAGIC    - Add the example SQL queries from Step 3 above
# MAGIC    - Add the general instructions text from Step 4 above
# MAGIC 7. Test with sample questions in the chat interface
# MAGIC 8. Note the Space ID from the URL for use in the Supervisor Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Genie Configuration for Supervisor Agent

# COMMAND ----------

genie_config = {
    "space_id": space_id,
    "space_name": GENIE_SPACE_NAME,
    "warehouse_id": SQL_WAREHOUSE_ID,
    "tables": [CUSTOMERS_TABLE, PRODUCTS_TABLE, ORDERS_TABLE, ORDER_ITEMS_TABLE, SUPPORT_TICKETS_TABLE],
}

print("\n" + "=" * 80)
print("GENIE SPACE CONFIGURATION (save for Supervisor Agent)")
print("=" * 80)
for k, v in genie_config.items():
    print(f"  {k}: {v}")

# Store for downstream notebooks (guard against None)
if space_id and space_id != "PLACEHOLDER_SET_MANUALLY":
    spark.conf.set("ka_genie.genie_space_id", space_id)
    spark.conf.set("ka_genie.warehouse_id", SQL_WAREHOUSE_ID)
else:
    print("\nNOTE: Space ID not set in spark conf. Set it manually in the Supervisor Agent notebook.")
