# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Create Synthetic Structured Data (Delta Tables)
# MAGIC
# MAGIC This notebook creates 5 interrelated Delta tables for an e-commerce domain:
# MAGIC - **customers** — customer demographics and account info
# MAGIC - **products** — product catalog with categories and pricing
# MAGIC - **orders** — order transactions
# MAGIC - **order_items** — line items linking orders to products
# MAGIC - **support_tickets** — customer support interactions
# MAGIC
# MAGIC These tables are designed to support analytical, lookup, and join queries via Genie.

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog, Schema, and Volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}
""")
print(f"Created catalog={CATALOG}, schema={SCHEMA}, volume={VOLUME_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers Table

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("membership_tier", StringType(), True),
    StructField("signup_date", DateType(), False),
    StructField("lifetime_value", DoubleType(), True),
])

customers_data = [
    ("C001", "Alice", "Johnson", "alice.johnson@email.com", "555-0101", "Seattle", "WA", "US", "Gold", datetime(2022, 1, 15).date(), 4520.50),
    ("C002", "Bob", "Smith", "bob.smith@email.com", "555-0102", "Portland", "OR", "US", "Silver", datetime(2022, 3, 22).date(), 1890.25),
    ("C003", "Clara", "Davis", "clara.davis@email.com", "555-0103", "San Francisco", "CA", "US", "Platinum", datetime(2021, 6, 10).date(), 12340.00),
    ("C004", "David", "Wilson", "david.wilson@email.com", "555-0104", "Austin", "TX", "US", "Gold", datetime(2022, 8, 5).date(), 3210.75),
    ("C005", "Eva", "Martinez", "eva.martinez@email.com", "555-0105", "Denver", "CO", "US", "Silver", datetime(2023, 1, 18).date(), 980.00),
    ("C006", "Frank", "Lee", "frank.lee@email.com", "555-0106", "Chicago", "IL", "US", "Bronze", datetime(2023, 4, 2).date(), 450.30),
    ("C007", "Grace", "Chen", "grace.chen@email.com", "555-0107", "New York", "NY", "US", "Platinum", datetime(2021, 2, 28).date(), 18920.00),
    ("C008", "Henry", "Taylor", "henry.taylor@email.com", "555-0108", "Miami", "FL", "US", "Gold", datetime(2022, 11, 14).date(), 5670.40),
    ("C009", "Irene", "Nguyen", "irene.nguyen@email.com", "555-0109", "Boston", "MA", "US", "Silver", datetime(2023, 7, 9).date(), 1560.80),
    ("C010", "Jack", "Brown", "jack.brown@email.com", "555-0110", "Phoenix", "AZ", "US", "Bronze", datetime(2023, 9, 21).date(), 320.00),
    ("C011", "Karen", "White", "karen.white@email.com", "555-0111", "Atlanta", "GA", "US", "Gold", datetime(2022, 5, 30).date(), 4100.60),
    ("C012", "Leo", "Garcia", "leo.garcia@email.com", "555-0112", "Dallas", "TX", "US", "Silver", datetime(2023, 2, 14).date(), 2340.90),
    ("C013", "Maya", "Anderson", "maya.anderson@email.com", "555-0113", "Minneapolis", "MN", "US", "Platinum", datetime(2021, 9, 5).date(), 15780.25),
    ("C014", "Nathan", "Thomas", "nathan.thomas@email.com", "555-0114", "Nashville", "TN", "US", "Bronze", datetime(2024, 1, 8).date(), 180.00),
    ("C015", "Olivia", "Jackson", "olivia.jackson@email.com", "555-0115", "Charlotte", "NC", "US", "Gold", datetime(2022, 7, 19).date(), 6230.15),
]

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
customers_df.write.mode("overwrite").saveAsTable(CUSTOMERS_TABLE)
print(f"Created {CUSTOMERS_TABLE} with {customers_df.count()} rows")
customers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Products Table

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), False),
    StructField("cost", DoubleType(), True),
    StructField("weight_kg", DoubleType(), True),
    StructField("warranty_years", IntegerType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("launch_date", DateType(), True),
])

products_data = [
    ("P001", "UltraBook Pro 15", "Electronics", "Laptops", "TechNova", 1299.99, 780.00, 1.8, 2, True, datetime(2023, 3, 1).date()),
    ("P002", "SwiftPad Tablet 10", "Electronics", "Tablets", "TechNova", 599.99, 320.00, 0.5, 1, True, datetime(2023, 6, 15).date()),
    ("P003", "CloudBuds Wireless", "Electronics", "Audio", "SoundWave", 149.99, 45.00, 0.05, 1, True, datetime(2023, 1, 10).date()),
    ("P004", "ErgoDesk Standing Desk", "Furniture", "Desks", "WorkWell", 799.99, 400.00, 35.0, 5, True, datetime(2022, 9, 1).date()),
    ("P005", "ComfortElite Chair", "Furniture", "Chairs", "WorkWell", 549.99, 250.00, 18.0, 3, True, datetime(2022, 11, 15).date()),
    ("P006", "PowerStation 1000W", "Electronics", "Power", "VoltMax", 249.99, 120.00, 3.2, 2, True, datetime(2023, 8, 20).date()),
    ("P007", "AquaPure Filter Bottle", "Home & Kitchen", "Drinkware", "PureLife", 39.99, 12.00, 0.3, 1, True, datetime(2023, 4, 5).date()),
    ("P008", "SmartFit Watch X", "Electronics", "Wearables", "TechNova", 349.99, 150.00, 0.08, 1, True, datetime(2023, 10, 1).date()),
    ("P009", "ProLens Camera Kit", "Electronics", "Photography", "OptiView", 899.99, 480.00, 1.2, 2, True, datetime(2022, 6, 1).date()),
    ("P010", "ZenMat Yoga Mat", "Sports", "Fitness", "FitZone", 69.99, 20.00, 1.5, 0, True, datetime(2023, 2, 14).date()),
    ("P011", "ThermoMug Pro", "Home & Kitchen", "Drinkware", "PureLife", 29.99, 8.00, 0.4, 1, True, datetime(2023, 5, 1).date()),
    ("P012", "UltraBook Air 13", "Electronics", "Laptops", "TechNova", 999.99, 580.00, 1.2, 2, True, datetime(2024, 1, 15).date()),
    ("P013", "SoundBar Elite", "Electronics", "Audio", "SoundWave", 399.99, 180.00, 4.5, 2, True, datetime(2023, 7, 1).date()),
    ("P014", "BackPack Pro Travel", "Accessories", "Bags", "TrekGear", 129.99, 40.00, 0.9, 1, True, datetime(2023, 3, 20).date()),
    ("P015", "DeskLamp LED Smart", "Furniture", "Lighting", "WorkWell", 89.99, 30.00, 1.1, 2, True, datetime(2023, 9, 10).date()),
]

products_df = spark.createDataFrame(products_data, schema=products_schema)
products_df.write.mode("overwrite").saveAsTable(PRODUCTS_TABLE)
print(f"Created {PRODUCTS_TABLE} with {products_df.count()} rows")
products_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Orders Table

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("shipping_method", StringType(), True),
    StructField("total_amount", DoubleType(), False),
    StructField("discount_pct", DoubleType(), True),
    StructField("payment_method", StringType(), True),
])

orders_data = [
    ("ORD-1001", "C001", datetime(2024, 1, 5).date(), "Delivered", "Standard", 1449.98, 0.0, "Credit Card"),
    ("ORD-1002", "C003", datetime(2024, 1, 12).date(), "Delivered", "Express", 599.99, 10.0, "PayPal"),
    ("ORD-1003", "C002", datetime(2024, 1, 18).date(), "Delivered", "Standard", 189.98, 0.0, "Credit Card"),
    ("ORD-1004", "C007", datetime(2024, 2, 3).date(), "Delivered", "Express", 2099.97, 5.0, "Credit Card"),
    ("ORD-1005", "C005", datetime(2024, 2, 14).date(), "Delivered", "Standard", 69.99, 0.0, "Debit Card"),
    ("ORD-1006", "C001", datetime(2024, 2, 20).date(), "Delivered", "Express", 349.99, 0.0, "Credit Card"),
    ("ORD-1007", "C004", datetime(2024, 3, 1).date(), "Delivered", "Standard", 879.98, 0.0, "PayPal"),
    ("ORD-1008", "C008", datetime(2024, 3, 10).date(), "Returned", "Standard", 149.99, 0.0, "Credit Card"),
    ("ORD-1009", "C003", datetime(2024, 3, 15).date(), "Delivered", "Express", 1299.99, 15.0, "Credit Card"),
    ("ORD-1010", "C006", datetime(2024, 3, 22).date(), "Delivered", "Standard", 39.99, 0.0, "Debit Card"),
    ("ORD-1011", "C011", datetime(2024, 4, 2).date(), "Delivered", "Express", 1349.98, 0.0, "Credit Card"),
    ("ORD-1012", "C009", datetime(2024, 4, 10).date(), "Processing", "Standard", 449.98, 5.0, "PayPal"),
    ("ORD-1013", "C013", datetime(2024, 4, 18).date(), "Shipped", "Express", 899.99, 10.0, "Credit Card"),
    ("ORD-1014", "C012", datetime(2024, 4, 25).date(), "Delivered", "Standard", 159.98, 0.0, "Debit Card"),
    ("ORD-1015", "C007", datetime(2024, 5, 1).date(), "Delivered", "Express", 549.99, 0.0, "Credit Card"),
    ("ORD-1016", "C015", datetime(2024, 5, 8).date(), "Delivered", "Standard", 799.99, 0.0, "Credit Card"),
    ("ORD-1017", "C001", datetime(2024, 5, 15).date(), "Delivered", "Express", 249.99, 0.0, "PayPal"),
    ("ORD-1018", "C010", datetime(2024, 5, 20).date(), "Cancelled", "Standard", 129.99, 0.0, "Debit Card"),
    ("ORD-1019", "C004", datetime(2024, 6, 1).date(), "Delivered", "Express", 1299.99, 5.0, "Credit Card"),
    ("ORD-1020", "C003", datetime(2024, 6, 10).date(), "Delivered", "Standard", 469.98, 0.0, "Credit Card"),
    ("ORD-1021", "C014", datetime(2024, 6, 15).date(), "Processing", "Standard", 69.99, 0.0, "Debit Card"),
    ("ORD-1022", "C008", datetime(2024, 6, 20).date(), "Delivered", "Express", 999.99, 10.0, "Credit Card"),
    ("ORD-1023", "C002", datetime(2024, 7, 1).date(), "Delivered", "Standard", 349.99, 0.0, "PayPal"),
    ("ORD-1024", "C011", datetime(2024, 7, 10).date(), "Shipped", "Express", 599.99, 0.0, "Credit Card"),
    ("ORD-1025", "C005", datetime(2024, 7, 15).date(), "Delivered", "Standard", 89.99, 0.0, "Debit Card"),
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
orders_df.write.mode("overwrite").saveAsTable(ORDERS_TABLE)
print(f"Created {ORDERS_TABLE} with {orders_df.count()} rows")
orders_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Order Items Table

# COMMAND ----------

order_items_schema = StructType([
    StructField("item_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("line_total", DoubleType(), False),
])

order_items_data = [
    ("LI-001", "ORD-1001", "P001", 1, 1299.99, 1299.99),
    ("LI-002", "ORD-1001", "P003", 1, 149.99, 149.99),
    ("LI-003", "ORD-1002", "P002", 1, 599.99, 599.99),
    ("LI-004", "ORD-1003", "P003", 1, 149.99, 149.99),
    ("LI-005", "ORD-1003", "P007", 1, 39.99, 39.99),
    ("LI-006", "ORD-1004", "P001", 1, 1299.99, 1299.99),
    ("LI-007", "ORD-1004", "P004", 1, 799.99, 799.99),
    ("LI-008", "ORD-1005", "P010", 1, 69.99, 69.99),
    ("LI-009", "ORD-1006", "P008", 1, 349.99, 349.99),
    ("LI-010", "ORD-1007", "P005", 1, 549.99, 549.99),
    ("LI-011", "ORD-1007", "P011", 1, 29.99, 29.99),
    ("LI-012", "ORD-1007", "P015", 1, 89.99, 89.99),
    ("LI-013", "ORD-1008", "P003", 1, 149.99, 149.99),
    ("LI-014", "ORD-1009", "P001", 1, 1299.99, 1299.99),
    ("LI-015", "ORD-1010", "P007", 1, 39.99, 39.99),
    ("LI-016", "ORD-1011", "P001", 1, 1299.99, 1299.99),
    ("LI-017", "ORD-1011", "P011", 1, 29.99, 29.99),
    ("LI-018", "ORD-1012", "P013", 1, 399.99, 399.99),
    ("LI-019", "ORD-1012", "P011", 1, 29.99, 29.99),
    ("LI-020", "ORD-1013", "P009", 1, 899.99, 899.99),
    ("LI-021", "ORD-1014", "P014", 1, 129.99, 129.99),
    ("LI-022", "ORD-1014", "P011", 1, 29.99, 29.99),
    ("LI-023", "ORD-1015", "P005", 1, 549.99, 549.99),
    ("LI-024", "ORD-1016", "P004", 1, 799.99, 799.99),
    ("LI-025", "ORD-1017", "P006", 1, 249.99, 249.99),
    ("LI-026", "ORD-1018", "P014", 1, 129.99, 129.99),
    ("LI-027", "ORD-1019", "P012", 1, 999.99, 999.99),
    ("LI-028", "ORD-1019", "P015", 1, 89.99, 89.99),
    ("LI-029", "ORD-1020", "P013", 1, 399.99, 399.99),
    ("LI-030", "ORD-1020", "P010", 1, 69.99, 69.99),
    ("LI-031", "ORD-1021", "P010", 1, 69.99, 69.99),
    ("LI-032", "ORD-1022", "P012", 1, 999.99, 999.99),
    ("LI-033", "ORD-1023", "P008", 1, 349.99, 349.99),
    ("LI-034", "ORD-1024", "P002", 1, 599.99, 599.99),
    ("LI-035", "ORD-1025", "P015", 1, 89.99, 89.99),
]

order_items_df = spark.createDataFrame(order_items_data, schema=order_items_schema)
order_items_df.write.mode("overwrite").saveAsTable(ORDER_ITEMS_TABLE)
print(f"Created {ORDER_ITEMS_TABLE} with {order_items_df.count()} rows")
order_items_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Support Tickets Table

# COMMAND ----------

support_tickets_schema = StructType([
    StructField("ticket_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), False),
    StructField("subject", StringType(), False),
    StructField("description", StringType(), True),
    StructField("priority", StringType(), False),
    StructField("status", StringType(), False),
    StructField("created_date", DateType(), False),
    StructField("resolved_date", DateType(), True),
    StructField("satisfaction_score", IntegerType(), True),
])

support_tickets_data = [
    ("TK-5001", "C008", "ORD-1008", "P003", "Return", "Defective CloudBuds - Left earbud not working",
     "Left earbud stopped producing sound after 2 weeks of use. Tried resetting per manual instructions but issue persists.",
     "High", "Resolved", datetime(2024, 3, 12).date(), datetime(2024, 3, 18).date(), 4),
    ("TK-5002", "C002", None, "P001", "Product Inquiry", "UltraBook Pro 15 RAM upgrade question",
     "Can I upgrade the RAM on the UltraBook Pro 15 from 16GB to 32GB? The product manual doesn't clearly state this.",
     "Low", "Resolved", datetime(2024, 2, 5).date(), datetime(2024, 2, 6).date(), 5),
    ("TK-5003", "C005", "ORD-1005", "P010", "Shipping", "ZenMat Yoga Mat arrived damaged",
     "The yoga mat arrived with a tear along one edge. Packaging appeared to be intact so it may be a manufacturing defect.",
     "Medium", "Resolved", datetime(2024, 2, 20).date(), datetime(2024, 2, 28).date(), 3),
    ("TK-5004", "C010", "ORD-1018", None, "Order", "Cancellation request for ORD-1018",
     "I need to cancel my order. I placed it by mistake. Please process the cancellation and refund.",
     "Medium", "Resolved", datetime(2024, 5, 21).date(), datetime(2024, 5, 22).date(), 5),
    ("TK-5005", "C003", "ORD-1009", "P001", "Warranty", "UltraBook Pro screen flickering",
     "Screen has intermittent flickering issue. Laptop is 3 months old. Requesting warranty repair or replacement.",
     "High", "Open", datetime(2024, 4, 1).date(), None, None),
    ("TK-5006", "C007", "ORD-1004", "P004", "Product Inquiry", "ErgoDesk assembly instructions unclear",
     "Step 7 in the assembly manual is confusing. The diagram doesn't match the parts received. Need clarification.",
     "Low", "Resolved", datetime(2024, 2, 10).date(), datetime(2024, 2, 11).date(), 4),
    ("TK-5007", "C001", "ORD-1006", "P008", "Return", "SmartFit Watch X battery drain issue",
     "Battery drains completely within 4 hours of full charge. Product manual states 48-hour battery life. Want a return.",
     "High", "Resolved", datetime(2024, 3, 5).date(), datetime(2024, 3, 15).date(), 3),
    ("TK-5008", "C011", "ORD-1011", "P001", "Warranty", "UltraBook keyboard key stuck",
     "The 'E' key is stuck and requires excessive force to press. Within warranty period. Need repair.",
     "Medium", "Open", datetime(2024, 4, 15).date(), None, None),
    ("TK-5009", "C015", "ORD-1016", "P004", "Shipping", "ErgoDesk delivery delayed",
     "Order shows delivered but I haven't received it. Tracking shows it was left at a different address.",
     "High", "Resolved", datetime(2024, 5, 12).date(), datetime(2024, 5, 20).date(), 2),
    ("TK-5010", "C004", None, None, "General", "Membership tier upgrade inquiry",
     "I've been a Gold member for 2 years. What are the requirements to upgrade to Platinum? What benefits do I get?",
     "Low", "Resolved", datetime(2024, 3, 20).date(), datetime(2024, 3, 21).date(), 5),
    ("TK-5011", "C009", "ORD-1012", "P013", "Product Inquiry", "SoundBar Elite compatibility question",
     "Will the SoundBar Elite work with my TV that only has optical audio output? Manual mentions HDMI ARC.",
     "Low", "Open", datetime(2024, 4, 12).date(), None, None),
    ("TK-5012", "C012", "ORD-1014", "P014", "Return", "BackPack Pro zipper broken on arrival",
     "Main compartment zipper was broken when I received the backpack. Requesting immediate replacement.",
     "Medium", "Resolved", datetime(2024, 4, 28).date(), datetime(2024, 5, 5).date(), 4),
]

support_tickets_df = spark.createDataFrame(support_tickets_data, schema=support_tickets_schema)
support_tickets_df.write.mode("overwrite").saveAsTable(SUPPORT_TICKETS_TABLE)
print(f"Created {SUPPORT_TICKETS_TABLE} with {support_tickets_df.count()} rows")
support_tickets_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Table & Column Comments (Semantic Layer for Genie)
# MAGIC These comments help Genie understand the business meaning of each table and column.

# COMMAND ----------

# -- Customers Table Comments --
spark.sql(f"COMMENT ON TABLE {CUSTOMERS_TABLE} IS 'Customer master data including demographics, membership tier (Bronze/Silver/Gold/Platinum), and lifetime value in USD.'")
spark.sql(f"ALTER TABLE {CUSTOMERS_TABLE} ALTER COLUMN customer_id COMMENT 'Unique customer identifier (C001, C002, ...)'")
spark.sql(f"ALTER TABLE {CUSTOMERS_TABLE} ALTER COLUMN membership_tier COMMENT 'Loyalty tier: Bronze, Silver, Gold, or Platinum. Higher tiers get better discounts and perks.'")
spark.sql(f"ALTER TABLE {CUSTOMERS_TABLE} ALTER COLUMN lifetime_value COMMENT 'Total amount spent by this customer across all orders, in USD.'")
spark.sql(f"ALTER TABLE {CUSTOMERS_TABLE} ALTER COLUMN signup_date COMMENT 'Date the customer created their account.'")

# -- Products Table Comments --
spark.sql(f"COMMENT ON TABLE {PRODUCTS_TABLE} IS 'Product catalog with pricing, cost, brand, category, warranty info. Categories: Electronics, Furniture, Home & Kitchen, Sports, Accessories.'")
spark.sql(f"ALTER TABLE {PRODUCTS_TABLE} ALTER COLUMN product_id COMMENT 'Unique product identifier (P001, P002, ...)'")
spark.sql(f"ALTER TABLE {PRODUCTS_TABLE} ALTER COLUMN price COMMENT 'Retail price in USD.'")
spark.sql(f"ALTER TABLE {PRODUCTS_TABLE} ALTER COLUMN cost COMMENT 'Cost to the company in USD. Margin = price - cost.'")
spark.sql(f"ALTER TABLE {PRODUCTS_TABLE} ALTER COLUMN warranty_years COMMENT 'Warranty duration in years. 0 means no warranty.'")
spark.sql(f"ALTER TABLE {PRODUCTS_TABLE} ALTER COLUMN is_active COMMENT 'Whether the product is currently available for sale.'")

# -- Orders Table Comments --
spark.sql(f"COMMENT ON TABLE {ORDERS_TABLE} IS 'Customer orders. Status values: Processing, Shipped, Delivered, Returned, Cancelled.'")
spark.sql(f"ALTER TABLE {ORDERS_TABLE} ALTER COLUMN order_id COMMENT 'Unique order identifier (ORD-1001, ORD-1002, ...)'")
spark.sql(f"ALTER TABLE {ORDERS_TABLE} ALTER COLUMN status COMMENT 'Order status: Processing, Shipped, Delivered, Returned, or Cancelled.'")
spark.sql(f"ALTER TABLE {ORDERS_TABLE} ALTER COLUMN total_amount COMMENT 'Total order value in USD after discount.'")
spark.sql(f"ALTER TABLE {ORDERS_TABLE} ALTER COLUMN discount_pct COMMENT 'Discount percentage applied to the order (0-100).'")
spark.sql(f"ALTER TABLE {ORDERS_TABLE} ALTER COLUMN shipping_method COMMENT 'Shipping method: Standard (5-7 days) or Express (1-2 days).'")

# -- Order Items Table Comments --
spark.sql(f"COMMENT ON TABLE {ORDER_ITEMS_TABLE} IS 'Line items in each order, linking orders to products with quantity and pricing.'")
spark.sql(f"ALTER TABLE {ORDER_ITEMS_TABLE} ALTER COLUMN line_total COMMENT 'Total for this line item: quantity * unit_price, in USD.'")

# -- Support Tickets Table Comments --
spark.sql(f"COMMENT ON TABLE {SUPPORT_TICKETS_TABLE} IS 'Customer support tickets. Categories: Return, Warranty, Shipping, Product Inquiry, Order, General. Priority: Low/Medium/High.'")
spark.sql(f"ALTER TABLE {SUPPORT_TICKETS_TABLE} ALTER COLUMN category COMMENT 'Ticket category: Return, Warranty, Shipping, Product Inquiry, Order, or General.'")
spark.sql(f"ALTER TABLE {SUPPORT_TICKETS_TABLE} ALTER COLUMN priority COMMENT 'Ticket priority: Low, Medium, or High.'")
spark.sql(f"ALTER TABLE {SUPPORT_TICKETS_TABLE} ALTER COLUMN status COMMENT 'Ticket status: Open or Resolved.'")
spark.sql(f"ALTER TABLE {SUPPORT_TICKETS_TABLE} ALTER COLUMN satisfaction_score COMMENT 'Customer satisfaction rating 1-5 (5 = best). NULL if ticket is still open.'")

print("All table and column comments added successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Queries (for Genie instructions)

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Top 5 customers by lifetime value
# MAGIC SELECT customer_id, first_name, last_name, membership_tier, lifetime_value
# MAGIC FROM ka_genie_demo.ecommerce.customers
# MAGIC ORDER BY lifetime_value DESC
# MAGIC LIMIT 5;
# MAGIC
# MAGIC -- Monthly revenue trend
# MAGIC SELECT DATE_TRUNC('month', order_date) AS month,
# MAGIC        COUNT(*) AS order_count,
# MAGIC        SUM(total_amount) AS revenue
# MAGIC FROM ka_genie_demo.ecommerce.orders
# MAGIC WHERE status = 'Delivered'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;
# MAGIC
# MAGIC -- Best-selling products by quantity
# MAGIC SELECT p.product_name, p.category, COUNT(oi.item_id) AS times_ordered,
# MAGIC        SUM(oi.line_total) AS total_revenue
# MAGIC FROM ka_genie_demo.ecommerce.order_items oi
# MAGIC JOIN ka_genie_demo.ecommerce.products p ON oi.product_id = p.product_id
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY times_ordered DESC;
# MAGIC
# MAGIC -- Support ticket resolution metrics
# MAGIC SELECT category, COUNT(*) AS total_tickets,
# MAGIC        SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END) AS resolved,
# MAGIC        AVG(satisfaction_score) AS avg_satisfaction
# MAGIC FROM ka_genie_demo.ecommerce.support_tickets
# MAGIC GROUP BY 1;
# MAGIC
# MAGIC -- Products with warranty claims
# MAGIC SELECT p.product_name, p.warranty_years, COUNT(t.ticket_id) AS warranty_tickets
# MAGIC FROM ka_genie_demo.ecommerce.support_tickets t
# MAGIC JOIN ka_genie_demo.ecommerce.products p ON t.product_id = p.product_id
# MAGIC WHERE t.category = 'Warranty'
# MAGIC GROUP BY 1, 2;
# MAGIC ```
