# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration for Genie + Knowledge Assistant Supervisor Agent Demo
# MAGIC Update these values to match your Databricks workspace.

# COMMAND ----------

# -- Unity Catalog Configuration --
CATALOG = "ka_genie_demo"
SCHEMA = "ecommerce"

# -- Volume for PDF storage (Knowledge Assistant source) --
VOLUME_NAME = "documents"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

# -- Genie Space Configuration --
GENIE_SPACE_NAME = "E-Commerce Analytics"
GENIE_SPACE_DESCRIPTION = (
    "Query structured e-commerce data including customers, products, orders, "
    "and support tickets. Use this space to answer questions about sales metrics, "
    "customer behavior, order trends, and support analytics."
)

# -- Knowledge Assistant Configuration --
KA_NAME = "E-Commerce Knowledge Base"
KA_DESCRIPTION = (
    "Answer questions about product specifications, return/refund policies, "
    "warranty terms, shipping guidelines, and frequently asked questions "
    "using company documentation."
)
KA_INSTRUCTIONS = (
    "You are a helpful e-commerce assistant. When answering questions, cite the "
    "specific document and section your answer comes from. If the documents do not "
    "contain the answer, say so clearly."
)

# -- Supervisor Agent Configuration --
SUPERVISOR_NAME = "E-Commerce Supervisor Agent"
SUPERVISOR_DESCRIPTION = (
    "A unified assistant that answers questions about e-commerce operations by "
    "combining structured data analytics (via Genie) with unstructured knowledge "
    "(via Knowledge Assistant). Routes queries intelligently to the right source."
)

# -- SQL Warehouse (for Genie) --
# Set this after provisioning or discovering your warehouse
SQL_WAREHOUSE_ID = "148ccb90800933a1"  # Shared Endpoint (azure11)

# -- Table FQNs --
CUSTOMERS_TABLE = f"{CATALOG}.{SCHEMA}.customers"
PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.products"
ORDERS_TABLE = f"{CATALOG}.{SCHEMA}.orders"
ORDER_ITEMS_TABLE = f"{CATALOG}.{SCHEMA}.order_items"
SUPPORT_TICKETS_TABLE = f"{CATALOG}.{SCHEMA}.support_tickets"
