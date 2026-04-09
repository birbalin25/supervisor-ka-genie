# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-Source Test Queries
# MAGIC
# MAGIC These 10 queries require **both** structured data (Genie) and unstructured data (Knowledge Assistant)
# MAGIC to provide a complete answer. They vary from simple lookups to multi-step reasoning.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 1: Simple Lookup + Policy
# MAGIC **Query:** "Customer C008 returned their CloudBuds (order ORD-1008). What is the return window for electronics, and will they be charged a restocking fee?"
# MAGIC
# MAGIC **Genie:** Retrieves order ORD-1008 details (product=P003 CloudBuds Wireless, status=Returned)
# MAGIC **KA:** Returns the Return Policy — electronics have a 30-day return window; opened electronics incur a 15% restocking fee, but defective returns have no restocking fee.
# MAGIC **Synthesis:** The answer depends on whether the return was due to a defect (ticket TK-5001 confirms it was defective), so no restocking fee applies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 2: Product Spec + Data Lookup
# MAGIC **Query:** "Can the UltraBook Pro 15 RAM be upgraded? Also, how many customers have purchased this laptop?"
# MAGIC
# MAGIC **Genie:** Counts distinct customers who ordered product P001 (UltraBook Pro 15) from order_items.
# MAGIC **KA:** Product Catalog confirms RAM IS upgradeable (16GB to 32GB DDR5, accessible via bottom panel, 8 screws).
# MAGIC **Synthesis:** "Yes, RAM is upgradeable to 32GB. 4 customers have purchased this laptop."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 3: Warranty Reasoning
# MAGIC **Query:** "Clara Davis has an open support ticket about her UltraBook Pro screen flickering. Is this covered under warranty, and what are her options?"
# MAGIC
# MAGIC **Genie:** Retrieves ticket TK-5005 (customer C003, product P001, category=Warranty, created 2024-04-01) and order ORD-1009 (ordered 2024-03-15).
# MAGIC **KA:** Warranty doc states UltraBook Pro 15 has 2-year warranty covering "parts & labor" including component failure under normal use. Screen flickering is a manufacturing defect.
# MAGIC **Synthesis:** "The laptop is ~1 month old, well within the 2-year warranty. Screen flickering is covered. Clara can get free repair or replacement within 7-14 business days."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 4: Membership Tier + Benefits
# MAGIC **Query:** "David Wilson wants to know what he needs to do to upgrade from Gold to Platinum. What are his current spending stats and what additional benefits would he get?"
# MAGIC
# MAGIC **Genie:** Retrieves David Wilson (C004, Gold tier, lifetime_value=$3,210.75) and his order history totals.
# MAGIC **KA:** Membership Program Guide shows Platinum requires $10,000 annual spend. Benefits include free express shipping, same-day dispatch, dedicated rep, 10% discount, free furniture assembly.
# MAGIC **Synthesis:** "David has spent $3,210.75. He needs $6,789.25 more to reach Platinum. Key new benefits: free express shipping, 10% discount (up from 5%), dedicated account rep."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 5: Troubleshooting + Order Context
# MAGIC **Query:** "Alice Johnson reported that her SmartFit Watch X battery drains in 4 hours. What does her ticket say, and what troubleshooting steps should she follow?"
# MAGIC
# MAGIC **Genie:** Retrieves ticket TK-5007 (customer C001, product P008, order ORD-1006, category=Return, priority=High, status=Resolved).
# MAGIC **KA:** FAQ doc lists troubleshooting steps: check GPS mode, display brightness, sync frequency, firmware update. Product spec says 48-hour typical battery life. If unresolved, eligible for warranty replacement within 1 year.
# MAGIC **Synthesis:** "Alice's ticket TK-5007 was resolved. Troubleshooting steps include checking GPS mode (which can drain battery in ~8 hours), reducing brightness, and updating firmware. The Watch has a 1-year warranty for defective battery replacement."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 6: Shipping + Order Analysis
# MAGIC **Query:** "What shipping benefits does a Platinum member get compared to a Gold member? Show me the top 3 customers by order count and identify which ones would benefit most from a Platinum upgrade."
# MAGIC
# MAGIC **Genie:** Calculates top customers by order count (e.g., C001 with 3 orders, C003 with 3 orders, C007 with 2 orders) and their current tiers.
# MAGIC **KA:** Shipping Guidelines and Membership Guide detail: Platinum gets free express shipping ($14.99 savings per order) and same-day dispatch vs. Gold's free standard only.
# MAGIC **Synthesis:** "Platinum members save $14.99/order on express shipping and get same-day dispatch. Top customers: Alice Johnson (Gold, 3 orders), Clara Davis (Platinum, already there), Grace Chen (Platinum). Alice would benefit most — she'd save ~$45/year on express shipping alone."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 7: Multi-Product Support Analysis
# MAGIC **Query:** "Which products have the most support tickets, and for the top product, what does the troubleshooting guide recommend?"
# MAGIC
# MAGIC **Genie:** Aggregates support tickets by product: P001 (UltraBook Pro 15) = 3 tickets, P003 (CloudBuds) = 1, P004 (ErgoDesk) = 2, P008 (SmartFit Watch) = 1.
# MAGIC **KA:** For UltraBook Pro 15: FAQ covers RAM upgrade questions, warranty claims for screen/keyboard issues. Warranty doc covers 2-year parts & labor.
# MAGIC **Synthesis:** "UltraBook Pro 15 leads with 3 tickets (warranty claims for screen flickering, keyboard, and a RAM inquiry). Troubleshooting: screen issues are covered under 2-year warranty for free repair. RAM is user-upgradeable. Keyboard issues also covered."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 8: Complex Return Scenario
# MAGIC **Query:** "If Olivia Jackson (order ORD-1016) wants to return her ErgoDesk Standing Desk, what are her options? She has already assembled it."
# MAGIC
# MAGIC **Genie:** Retrieves order ORD-1016 (customer C015, product P004 ErgoDesk, status=Delivered, date=2024-05-08).
# MAGIC **KA:** Return Policy states assembled furniture is NOT returnable (only warranty claims). Furniture return window is 14 days if unassembled. Warranty covers 5 years (frame) and 3 years (motor).
# MAGIC **Synthesis:** "Since Olivia has already assembled the ErgoDesk, it cannot be returned. However, if there's a defect, she's covered by the 5-year frame / 3-year motor warranty for free repair. She should file a warranty claim if there's an issue."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 9: Revenue + Extended Warranty Recommendation
# MAGIC **Query:** "What is the total revenue from Electronics products, and what extended warranty options are available for customers who purchased laptops?"
# MAGIC
# MAGIC **Genie:** Calculates total revenue from Electronics category by joining products and order_items where category='Electronics' and order status='Delivered'.
# MAGIC **KA:** Warranty doc details extended warranty options: Basic Plus (+1yr, 8% of price), Premium Care (+2yr, 14%, includes accidental damage), Total Protection (+3yr, 20%, includes battery replacement). Must be purchased within 30 days.
# MAGIC **Synthesis:** "Electronics revenue is $X,XXX.XX. For UltraBook Pro 15 ($1,299.99), extended warranty options are: Basic Plus ($104/+1yr), Premium Care ($182/+2yr with accidental damage), Total Protection ($260/+3yr with battery). Must be bought within 30 days of purchase."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query 10: End-to-End Customer Journey Analysis
# MAGIC **Query:** "Give me a complete customer profile for Grace Chen — her orders, support history, membership benefits, and any relevant product details or policy information for her open issues."
# MAGIC
# MAGIC **Genie:** Retrieves customer C007 (Grace Chen, Platinum, NYC, lifetime_value=$18,920), her orders (ORD-1004: $2,099.97 UltraBook+ErgoDesk, ORD-1015: $549.99 ComfortElite Chair), and any support tickets (TK-5006: ErgoDesk assembly confusion, resolved).
# MAGIC **KA:** Platinum benefits: 10% discount, free express shipping, same-day dispatch, dedicated rep, 60-day returns. ErgoDesk assembly: Step 7 clarification available in FAQ. ErgoDesk warranty: 5yr frame, 3yr motor.
# MAGIC **Synthesis:** "Grace Chen is a Platinum member ($18,920 LTV) from NYC with 2 orders. She had one resolved ticket about ErgoDesk assembly (Step 7 cross-bar confusion). As Platinum, she enjoys 10% discounts, free express shipping, same-day dispatch, and 60-day returns. Her ErgoDesk has a 5-year frame warranty remaining."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Classification Matrix

# COMMAND ----------

queries = [
    {"id": 1, "complexity": "Simple", "genie": True, "ka": True, "description": "Return status + policy lookup"},
    {"id": 2, "complexity": "Simple", "genie": True, "ka": True, "description": "Product spec + customer count"},
    {"id": 3, "complexity": "Medium", "genie": True, "ka": True, "description": "Warranty eligibility reasoning"},
    {"id": 4, "complexity": "Medium", "genie": True, "ka": True, "description": "Membership tier gap analysis"},
    {"id": 5, "complexity": "Medium", "genie": True, "ka": True, "description": "Troubleshooting with ticket context"},
    {"id": 6, "complexity": "Complex", "genie": True, "ka": True, "description": "Shipping benefits + top customer analysis"},
    {"id": 7, "complexity": "Complex", "genie": True, "ka": True, "description": "Support analysis + troubleshooting guide"},
    {"id": 8, "complexity": "Complex", "genie": True, "ka": True, "description": "Return policy reasoning for edge case"},
    {"id": 9, "complexity": "Complex", "genie": True, "ka": True, "description": "Revenue + extended warranty recommendation"},
    {"id": 10, "complexity": "Complex", "genie": True, "ka": True, "description": "Full customer journey analysis"},
]

df = spark.createDataFrame(queries)
df.display()
