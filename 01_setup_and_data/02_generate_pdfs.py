# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Generate Synthetic Unstructured Data (PDFs)
# MAGIC
# MAGIC This notebook creates PDF documents that complement the structured Delta tables.
# MAGIC PDFs are uploaded to a Unity Catalog Volume for use by the Knowledge Assistant.
# MAGIC
# MAGIC **Documents generated:**
# MAGIC 1. Product Catalog & Specifications Guide
# MAGIC 2. Return, Refund & Exchange Policy
# MAGIC 3. Warranty Terms & Conditions
# MAGIC 4. Shipping & Delivery Guidelines
# MAGIC 5. Customer FAQ & Troubleshooting Guide
# MAGIC 6. Membership Program Guide

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %pip install fpdf2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-import after restart
import os
from fpdf import FPDF

# Re-run config values
CATALOG = "ka_genie_demo"
SCHEMA = "ecommerce"
VOLUME_NAME = "documents"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDF Generation Helpers

# COMMAND ----------

class StyledPDF(FPDF):
    """Custom PDF class with consistent styling for all documents."""

    def __init__(self, title, subtitle=""):
        super().__init__()
        self.doc_title = title
        self.doc_subtitle = subtitle
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, self.doc_title, align="R", new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def add_title_page(self):
        self.add_page()
        self.ln(60)
        self.set_font("Helvetica", "B", 28)
        self.set_text_color(0, 51, 102)
        self.multi_cell(0, 14, self.doc_title, align="C")
        if self.doc_subtitle:
            self.ln(5)
            self.set_font("Helvetica", "", 14)
            self.set_text_color(80, 80, 80)
            self.multi_cell(0, 10, self.doc_subtitle, align="C")
        self.ln(20)
        self.set_font("Helvetica", "I", 10)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, "TechCommerce Inc. | Confidential", align="C", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 10, "Last Updated: January 2024", align="C")

    def section_header(self, text, level=1):
        if level == 1:
            self.set_font("Helvetica", "B", 16)
            self.set_text_color(0, 51, 102)
            self.ln(8)
        elif level == 2:
            self.set_font("Helvetica", "B", 13)
            self.set_text_color(0, 80, 130)
            self.ln(5)
        else:
            self.set_font("Helvetica", "B", 11)
            self.set_text_color(50, 50, 50)
            self.ln(3)
        self.multi_cell(0, 8, text, new_x="LMARGIN", new_y="NEXT")
        if level == 1:
            self.set_draw_color(0, 51, 102)
            self.line(10, self.get_y(), 100, self.get_y())
        self.ln(3)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 6, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def bullet_list(self, items):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        for item in items:
            self.cell(5)
            self.cell(5, 6, "-")
            self.multi_cell(0, 6, f" {item}", new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def add_table(self, headers, data, col_widths=None):
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)
        # Header
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 51, 102)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 8, h, border=1, fill=True, align="C")
        self.ln()
        # Rows
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        fill = False
        for row in data:
            if fill:
                self.set_fill_color(240, 240, 240)
            else:
                self.set_fill_color(255, 255, 255)
            for i, cell in enumerate(row):
                self.cell(col_widths[i], 7, str(cell), border=1, fill=True)
            self.ln()
            fill = not fill
        self.ln(4)

    def note_box(self, text):
        self.set_fill_color(255, 243, 205)
        self.set_draw_color(255, 193, 7)
        self.set_font("Helvetica", "I", 9)
        self.set_text_color(80, 60, 0)
        x = self.get_x()
        y = self.get_y()
        self.rect(x, y, 190, 14, style="DF")
        self.set_xy(x + 3, y + 2)
        self.multi_cell(184, 5, f"Note: {text}")
        self.ln(4)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Document 1: Product Catalog & Specifications

# COMMAND ----------

def create_product_catalog():
    pdf = StyledPDF("Product Catalog & Specifications", "TechCommerce Inc. | 2024 Edition")
    pdf.alias_nb_pages()
    pdf.add_title_page()

    pdf.add_page()
    pdf.section_header("1. Electronics")

    # UltraBook Pro 15
    pdf.section_header("1.1 UltraBook Pro 15 (P001)", level=2)
    pdf.body_text(
        "The UltraBook Pro 15 is our flagship laptop, designed for professionals who demand performance "
        "and portability. Powered by the latest Intel Core i7 processor with 16GB DDR5 RAM and a 512GB "
        "NVMe SSD, it handles demanding workloads with ease."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Display", '15.6" IPS, 2560x1440, 120Hz'],
            ["Processor", "Intel Core i7-13700H"],
            ["RAM", "16GB DDR5 (upgradeable to 32GB)"],
            ["Storage", "512GB NVMe SSD (2 M.2 slots)"],
            ["Battery", "72Wh, up to 10 hours"],
            ["Weight", "1.8 kg"],
            ["OS", "Windows 11 Pro"],
            ["Ports", "2x USB-C, 1x USB-A, HDMI 2.1, SD card"],
            ["Warranty", "2 years standard"],
        ],
        col_widths=[50, 140],
    )
    pdf.note_box("RAM is user-upgradeable. Open the bottom panel (8 screws) to access the SODIMM slot.")

    # UltraBook Air 13
    pdf.section_header("1.2 UltraBook Air 13 (P012)", level=2)
    pdf.body_text(
        "The UltraBook Air 13 is our ultralight companion for on-the-go productivity. At just 1.2 kg, "
        "it features a stunning 13.3-inch OLED display and all-day battery life."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Display", '13.3" OLED, 2880x1800, 60Hz'],
            ["Processor", "Intel Core i5-1340P"],
            ["RAM", "16GB LPDDR5 (soldered, not upgradeable)"],
            ["Storage", "256GB NVMe SSD"],
            ["Battery", "58Wh, up to 14 hours"],
            ["Weight", "1.2 kg"],
            ["Warranty", "2 years standard"],
        ],
        col_widths=[50, 140],
    )
    pdf.note_box("RAM is NOT upgradeable on this model as it is soldered to the motherboard.")

    # SwiftPad Tablet 10
    pdf.section_header("1.3 SwiftPad Tablet 10 (P002)", level=2)
    pdf.body_text(
        "The SwiftPad Tablet 10 offers a versatile computing experience with a detachable keyboard "
        "and stylus support. Ideal for note-taking, media consumption, and light productivity."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Display", '10.5" LCD, 2560x1600'],
            ["Processor", "Qualcomm Snapdragon 8 Gen 2"],
            ["RAM", "8GB"],
            ["Storage", "128GB + microSD"],
            ["Battery", "8000mAh, up to 12 hours"],
            ["Weight", "0.5 kg"],
            ["Warranty", "1 year standard"],
        ],
        col_widths=[50, 140],
    )

    # CloudBuds Wireless
    pdf.section_header("1.4 CloudBuds Wireless (P003)", level=2)
    pdf.body_text(
        "CloudBuds Wireless earbuds deliver premium audio with active noise cancellation. "
        "IPX4 water resistance makes them suitable for workouts. Features Bluetooth 5.3 "
        "with multipoint connectivity for seamless device switching."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Driver", "10mm dynamic driver"],
            ["ANC", "Active Noise Cancellation with transparency mode"],
            ["Battery", "8 hrs earbuds + 24 hrs case"],
            ["Connectivity", "Bluetooth 5.3, multipoint"],
            ["Water Resistance", "IPX4"],
            ["Weight", "5g per earbud"],
            ["Warranty", "1 year standard"],
        ],
        col_widths=[50, 140],
    )
    pdf.body_text(
        "Troubleshooting: If one earbud stops working, place both buds in the charging case, "
        "hold the case button for 10 seconds until LEDs flash red, then re-pair. If issue "
        "persists after factory reset, contact support for warranty replacement."
    )

    # SmartFit Watch X
    pdf.section_header("1.5 SmartFit Watch X (P008)", level=2)
    pdf.body_text(
        "The SmartFit Watch X combines fitness tracking with smart notifications. Features "
        "include heart rate monitoring, GPS, SpO2, sleep tracking, and 100+ workout modes."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Display", '1.4" AMOLED, always-on'],
            ["Sensors", "HR, SpO2, accelerometer, gyroscope, GPS"],
            ["Battery", "Up to 48 hours (typical), 14 days (saver)"],
            ["Water Resistance", "5 ATM"],
            ["Compatibility", "iOS 14+ / Android 10+"],
            ["Warranty", "1 year standard"],
        ],
        col_widths=[50, 140],
    )
    pdf.note_box(
        "Battery life of 48 hours is based on typical use (HR monitoring on, GPS off, 100 notifications/day). "
        "Heavy GPS use reduces battery life to approximately 8 hours."
    )

    # SoundBar Elite & ProLens Camera
    pdf.section_header("1.6 SoundBar Elite (P013)", level=2)
    pdf.body_text(
        "The SoundBar Elite delivers immersive 5.1 virtual surround sound from a single bar. "
        "Connectivity options include HDMI ARC/eARC, optical (Toslink), Bluetooth 5.0, and WiFi. "
        "Compatible with Dolby Atmos and DTS:X audio formats."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Output Power", "300W total"],
            ["Channels", "5.1 virtual surround"],
            ["Inputs", "HDMI ARC/eARC, Optical, Bluetooth, WiFi"],
            ["Subwoofer", "Wireless 6.5 inch"],
            ["Dimensions", "90cm x 6cm x 8cm"],
            ["Warranty", "2 years standard"],
        ],
        col_widths=[50, 140],
    )
    pdf.note_box(
        "The SoundBar Elite supports BOTH optical and HDMI ARC connections. For TVs with only optical "
        "output, use the included optical cable. HDMI ARC is recommended for the best experience."
    )

    # PowerStation 1000W
    pdf.section_header("1.7 PowerStation 1000W (P006)", level=2)
    pdf.body_text(
        "The PowerStation 1000W portable power station is ideal for camping, emergencies, "
        "and remote work. Features a 1000Wh lithium battery with pure sine wave inverter."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Capacity", "1000Wh"],
            ["Output", "1000W (2000W surge)"],
            ["Ports", "3x AC, 2x USB-C PD 100W, 2x USB-A, 12V DC"],
            ["Charge Time", "2 hours (wall), 5 hours (solar 200W)"],
            ["Weight", "3.2 kg"],
            ["Warranty", "2 years standard"],
        ],
        col_widths=[50, 140],
    )

    pdf.add_page()
    pdf.section_header("2. Furniture")

    # ErgoDesk Standing Desk
    pdf.section_header("2.1 ErgoDesk Standing Desk (P004)", level=2)
    pdf.body_text(
        "The ErgoDesk Standing Desk features dual-motor electric height adjustment with 4 memory "
        "presets. The bamboo desktop is 140cm x 70cm and supports loads up to 120kg."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Desktop Size", "140cm x 70cm"],
            ["Height Range", "62cm - 127cm"],
            ["Motor", "Dual motor, < 50dB"],
            ["Load Capacity", "120 kg"],
            ["Material", "Bamboo top, steel frame"],
            ["Memory Presets", "4 programmable heights"],
            ["Warranty", "5 years (frame), 3 years (motor)"],
        ],
        col_widths=[50, 140],
    )
    pdf.body_text(
        "Assembly Instructions Summary: The ErgoDesk comes in 2 boxes and requires approximately "
        "45 minutes to assemble with 2 people. Tools needed: Phillips screwdriver (included), "
        "Allen key set (included). See the detailed assembly manual for step-by-step instructions "
        "with diagrams. Note: Step 7 involves attaching the cross-bar support -- align the bolt "
        "holes on the cross-bar with the pre-drilled holes on the leg columns before tightening."
    )

    # ComfortElite Chair
    pdf.section_header("2.2 ComfortElite Chair (P005)", level=2)
    pdf.body_text(
        "The ComfortElite Chair is an ergonomic office chair with adjustable lumbar support, "
        "4D armrests, and breathable mesh back. Designed for 8+ hours of comfortable sitting."
    )
    pdf.add_table(
        ["Specification", "Detail"],
        [
            ["Seat Height", "42cm - 52cm"],
            ["Max Weight", "150 kg"],
            ["Tilt", "Synchro-tilt with lock"],
            ["Armrests", "4D adjustable"],
            ["Headrest", "Adjustable height and angle"],
            ["Material", "Mesh back, foam seat"],
            ["Warranty", "3 years"],
        ],
        col_widths=[50, 140],
    )

    # DeskLamp LED Smart
    pdf.section_header("2.3 DeskLamp LED Smart (P015)", level=2)
    pdf.body_text(
        "The DeskLamp LED Smart offers adjustable color temperature (2700K-6500K) and brightness "
        "with app control. Features USB-C charging port in the base and auto-dimming ambient sensor."
    )

    pdf.add_page()
    pdf.section_header("3. Other Products")

    # AquaPure Filter Bottle
    pdf.section_header("3.1 AquaPure Filter Bottle (P007)", level=2)
    pdf.body_text(
        "The AquaPure Filter Bottle uses a 3-stage filtration system to remove 99.9% of bacteria "
        "and reduce chlorine, heavy metals, and particulates. Each filter lasts approximately 300 "
        "uses (about 2 months of daily use)."
    )

    # ZenMat Yoga Mat
    pdf.section_header("3.2 ZenMat Yoga Mat (P010)", level=2)
    pdf.body_text(
        "The ZenMat Yoga Mat is a 6mm thick, non-slip mat made from natural rubber and TPE. "
        "Dimensions: 183cm x 68cm. Includes a carrying strap. This product does not carry a "
        "warranty but has a 30-day satisfaction guarantee."
    )

    # ThermoMug Pro
    pdf.section_header("3.3 ThermoMug Pro (P011)", level=2)
    pdf.body_text(
        "The ThermoMug Pro keeps drinks hot for 6 hours or cold for 12 hours with double-wall "
        "vacuum insulation. 450ml capacity with leak-proof lid. Fits standard car cup holders."
    )

    # BackPack Pro Travel
    pdf.section_header("3.4 BackPack Pro Travel (P014)", level=2)
    pdf.body_text(
        "The BackPack Pro Travel is a 40L expandable backpack with dedicated laptop compartment "
        "(fits up to 16 inches), anti-theft hidden pocket, USB charging port, and luggage strap. "
        "TSA-friendly design opens flat for airport security checks."
    )

    return pdf


catalog_pdf = create_product_catalog()
catalog_pdf.output(f"{VOLUME_PATH}/product_catalog_and_specifications.pdf")
print("Created: product_catalog_and_specifications.pdf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document 2: Return, Refund & Exchange Policy

# COMMAND ----------

def create_return_policy():
    pdf = StyledPDF("Return, Refund & Exchange Policy", "TechCommerce Inc.")
    pdf.alias_nb_pages()
    pdf.add_title_page()

    pdf.add_page()
    pdf.section_header("1. Overview")
    pdf.body_text(
        "At TechCommerce Inc., we want you to be completely satisfied with your purchase. "
        "This policy outlines the terms under which you may return, exchange, or receive a "
        "refund for products purchased through our platform."
    )

    pdf.section_header("2. General Return Policy")
    pdf.body_text(
        "Most products can be returned within 30 days of delivery for a full refund or exchange. "
        "Items must be in their original packaging, unused, and in resalable condition."
    )
    pdf.add_table(
        ["Category", "Return Window", "Restocking Fee", "Condition"],
        [
            ["Electronics", "30 days", "None", "Unopened or defective"],
            ["Electronics (opened)", "30 days", "15%", "Like-new, all accessories"],
            ["Furniture", "14 days", "20%", "Unassembled, original packaging"],
            ["Furniture (assembled)", "Not returnable", "N/A", "Warranty claims only"],
            ["Accessories", "30 days", "None", "Unused, with tags"],
            ["Sports/Fitness", "30 days", "None", "Unused, with tags"],
            ["Home & Kitchen", "30 days", "None", "Unused, original packaging"],
        ],
        col_widths=[38, 32, 32, 88],
    )
    pdf.note_box(
        "Products purchased during promotional events (Black Friday, Prime Day) follow the same "
        "return policy. Promotional discounts are refunded at the price paid, not the original price."
    )

    pdf.section_header("3. Defective Product Returns")
    pdf.body_text(
        "Defective products may be returned at any time during the warranty period at no cost. "
        "We will provide a prepaid shipping label and either replace the item or issue a full refund, "
        "at your choice. No restocking fee applies to defective returns."
    )
    pdf.body_text("To initiate a defective product return:")
    pdf.bullet_list([
        "Contact our support team and describe the defect",
        "You may be asked to provide photos or videos of the issue",
        "A return label will be emailed within 24 hours",
        "Ship the item within 7 days of receiving the label",
        "Refund or replacement is processed within 3-5 business days of receipt",
    ])

    pdf.section_header("4. Refund Processing")
    pdf.body_text("Refunds are processed to the original payment method:")
    pdf.add_table(
        ["Payment Method", "Refund Timeline", "Notes"],
        [
            ["Credit Card", "5-7 business days", "After item received"],
            ["Debit Card", "5-10 business days", "After item received"],
            ["PayPal", "3-5 business days", "After item received"],
            ["Store Credit", "Immediate", "Upon return approval"],
        ],
        col_widths=[50, 50, 90],
    )

    pdf.section_header("5. Exchange Policy")
    pdf.body_text(
        "Exchanges are available for the same product in a different configuration (e.g., color, size) "
        "or for a different product of equal or greater value. If the replacement product costs more, "
        "you will be charged the difference. If it costs less, the difference is refunded."
    )

    pdf.section_header("6. Non-Returnable Items")
    pdf.body_text("The following items cannot be returned:")
    pdf.bullet_list([
        "Products with personalized or custom engravings",
        "Opened software or digital products",
        "Gift cards",
        "Products marked as 'Final Sale'",
        "Items damaged due to misuse, neglect, or unauthorized modifications",
        "Assembled furniture (covered under warranty for defects only)",
    ])

    pdf.section_header("7. Shipping Costs for Returns")
    pdf.body_text(
        "For standard returns (non-defective), the customer is responsible for return shipping costs. "
        "For defective products, TechCommerce provides a prepaid return label at no cost. "
        "Express shipping returns are available for an additional fee of $14.99."
    )

    pdf.section_header("8. How to Initiate a Return")
    pdf.body_text("Follow these steps to start a return:")
    pdf.bullet_list([
        "Log in to your TechCommerce account",
        "Navigate to Order History and select the order",
        "Click 'Return/Exchange' next to the item",
        "Select your reason and preferred resolution (refund/exchange)",
        "Print the return label and pack the item securely",
        "Drop off at any authorized shipping partner location",
    ])

    return pdf


return_pdf = create_return_policy()
return_pdf.output(f"{VOLUME_PATH}/return_refund_exchange_policy.pdf")
print("Created: return_refund_exchange_policy.pdf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document 3: Warranty Terms & Conditions

# COMMAND ----------

def create_warranty_doc():
    pdf = StyledPDF("Warranty Terms & Conditions", "TechCommerce Inc.")
    pdf.alias_nb_pages()
    pdf.add_title_page()

    pdf.add_page()
    pdf.section_header("1. Warranty Coverage Overview")
    pdf.body_text(
        "TechCommerce Inc. provides manufacturer warranties on all products sold through our platform. "
        "Warranty duration and terms vary by product category and brand."
    )
    pdf.add_table(
        ["Product", "Warranty Period", "Coverage Type"],
        [
            ["UltraBook Pro 15", "2 years", "Parts & labor"],
            ["UltraBook Air 13", "2 years", "Parts & labor"],
            ["SwiftPad Tablet 10", "1 year", "Parts & labor"],
            ["CloudBuds Wireless", "1 year", "Replacement"],
            ["SmartFit Watch X", "1 year", "Replacement"],
            ["ErgoDesk Standing Desk", "5 yrs frame / 3 yrs motor", "Parts & labor"],
            ["ComfortElite Chair", "3 years", "Parts & labor"],
            ["SoundBar Elite", "2 years", "Parts & labor"],
            ["ProLens Camera Kit", "2 years", "Parts & labor"],
            ["PowerStation 1000W", "2 years", "Replacement"],
            ["AquaPure Filter Bottle", "1 year", "Replacement"],
            ["ThermoMug Pro", "1 year", "Replacement"],
            ["BackPack Pro Travel", "1 year", "Replacement"],
            ["DeskLamp LED Smart", "2 years", "Replacement"],
            ["ZenMat Yoga Mat", "No warranty", "30-day guarantee"],
        ],
        col_widths=[60, 60, 70],
    )

    pdf.section_header("2. What Is Covered")
    pdf.body_text("The warranty covers:")
    pdf.bullet_list([
        "Manufacturing defects in materials or workmanship",
        "Component failure under normal use conditions",
        "Software/firmware issues present at time of purchase (electronics)",
        "Structural defects in furniture products",
    ])

    pdf.section_header("3. What Is NOT Covered")
    pdf.body_text("The warranty does NOT cover:")
    pdf.bullet_list([
        "Damage from accidents, drops, spills, or natural disasters",
        "Normal wear and tear (scratches, cosmetic blemishes)",
        "Unauthorized modifications or repairs",
        "Damage from use with incompatible accessories or power sources",
        "Battery degradation below 80% capacity (considered normal after 500 charge cycles)",
        "Software issues caused by third-party applications",
        "Consumable parts (filters, pads, cables) unless defective at purchase",
    ])

    pdf.section_header("4. Extended Warranty Options")
    pdf.body_text(
        "TechCommerce offers extended warranty plans that can be purchased within 30 days of "
        "the original product purchase:"
    )
    pdf.add_table(
        ["Plan", "Duration", "Price", "Coverage"],
        [
            ["Basic Plus", "+1 year", "8% of product price", "Same as standard warranty"],
            ["Premium Care", "+2 years", "14% of product price", "Standard + accidental damage"],
            ["Total Protection", "+3 years", "20% of product price", "Premium + battery replacement"],
        ],
        col_widths=[40, 30, 50, 70],
    )
    pdf.note_box(
        "Extended warranty must be purchased within 30 days of original product purchase. "
        "It begins after the standard warranty expires."
    )

    pdf.section_header("5. How to File a Warranty Claim")
    pdf.body_text("To file a warranty claim:")
    pdf.bullet_list([
        "Locate your order number and product serial number",
        "Contact TechCommerce Support via phone (1-800-TECH-COM) or email (warranty@techcommerce.com)",
        "Describe the issue in detail; photos/videos may be requested",
        "Support will determine if the issue is covered under warranty",
        "If covered: receive a prepaid shipping label (or on-site service for furniture)",
        "Repair or replacement typically completed within 7-14 business days",
    ])

    pdf.section_header("6. Warranty Repair vs. Replacement")
    pdf.body_text(
        "For products with 'Parts & labor' coverage, TechCommerce will first attempt to repair "
        "the product. If repair is not feasible, a replacement of equal or better specification "
        "will be provided. For 'Replacement' coverage products, a new unit is shipped directly "
        "and the defective unit must be returned within 14 days."
    )

    pdf.section_header("7. Warranty Transfer")
    pdf.body_text(
        "Warranties are transferable to a new owner if the product is resold or gifted. The new "
        "owner must provide the original order number and contact support to register the transfer. "
        "The warranty period does not reset upon transfer."
    )

    return pdf


warranty_pdf = create_warranty_doc()
warranty_pdf.output(f"{VOLUME_PATH}/warranty_terms_and_conditions.pdf")
print("Created: warranty_terms_and_conditions.pdf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document 4: Shipping & Delivery Guidelines

# COMMAND ----------

def create_shipping_doc():
    pdf = StyledPDF("Shipping & Delivery Guidelines", "TechCommerce Inc.")
    pdf.alias_nb_pages()
    pdf.add_title_page()

    pdf.add_page()
    pdf.section_header("1. Shipping Methods & Timelines")
    pdf.body_text(
        "TechCommerce offers two shipping methods for domestic orders within the United States. "
        "Shipping times are calculated from the date of shipment, not the order date."
    )
    pdf.add_table(
        ["Method", "Delivery Time", "Cost", "Weight Limit"],
        [
            ["Standard", "5-7 business days", "Free (orders $50+) / $5.99", "Up to 70 lbs"],
            ["Express", "1-2 business days", "$14.99 / Free (Platinum)", "Up to 70 lbs"],
        ],
        col_widths=[35, 45, 60, 50],
    )
    pdf.note_box(
        "Platinum members receive free Express shipping on all orders. Gold members receive "
        "free Standard shipping with no minimum order amount."
    )

    pdf.section_header("2. Order Processing Time")
    pdf.body_text(
        "Orders placed before 2:00 PM EST are processed the same business day. Orders placed "
        "after 2:00 PM EST or on weekends/holidays are processed the next business day. "
        "Furniture items (ErgoDesk, ComfortElite Chair) require an additional 1-2 business days "
        "for warehouse preparation due to their size and packaging requirements."
    )

    pdf.section_header("3. Shipping Costs by Membership Tier")
    pdf.add_table(
        ["Tier", "Standard Shipping", "Express Shipping", "Other Benefits"],
        [
            ["Bronze", "$5.99 (free over $50)", "$14.99", "None"],
            ["Silver", "$3.99 (free over $35)", "$12.99", "Free gift wrapping"],
            ["Gold", "Free", "$9.99", "Priority processing"],
            ["Platinum", "Free", "Free", "Same-day dispatch"],
        ],
        col_widths=[30, 45, 45, 70],
    )

    pdf.section_header("4. Tracking Your Order")
    pdf.body_text(
        "Once your order ships, you will receive an email with a tracking number. You can "
        "track your shipment through:"
    )
    pdf.bullet_list([
        "Your TechCommerce account > Order History > Track Order",
        "The tracking link in your shipment confirmation email",
        "Directly on the carrier's website (UPS, FedEx, or USPS)",
    ])

    pdf.section_header("5. Delivery Issues")

    pdf.section_header("5.1 Missing Package", level=2)
    pdf.body_text(
        "If your package shows as 'Delivered' but you haven't received it, wait 24 hours as "
        "carriers sometimes mark packages delivered prematurely. Check with neighbors, building "
        "management, or common delivery areas. If still missing after 48 hours, contact support "
        "with your order number -- we will file a claim with the carrier and either reship or refund."
    )

    pdf.section_header("5.2 Damaged in Transit", level=2)
    pdf.body_text(
        "If your package arrives with visible damage, take photos of both the packaging and "
        "the product before discarding any materials. Contact support within 48 hours of delivery. "
        "We will arrange a free return and send a replacement at no additional cost."
    )

    pdf.section_header("5.3 Wrong Item Received", level=2)
    pdf.body_text(
        "If you receive the wrong item, do not open or use it. Contact support immediately. "
        "We will send the correct item via Express shipping and provide a prepaid label to "
        "return the incorrect item."
    )

    pdf.section_header("6. Large Item Delivery (Furniture)")
    pdf.body_text(
        "Furniture items (ErgoDesk Standing Desk, ComfortElite Chair) are shipped via freight "
        "carrier and delivered to your doorstep (curbside delivery). Inside delivery and "
        "assembly services are available for an additional fee:"
    )
    pdf.add_table(
        ["Service", "Cost", "Description"],
        [
            ["Curbside Delivery", "Included", "Delivered to front door/loading dock"],
            ["Room of Choice", "$29.99", "Placed in room of your choice"],
            ["Assembly", "$79.99", "Full assembly by certified technician"],
        ],
        col_widths=[45, 35, 110],
    )

    pdf.section_header("7. Holiday & Peak Season Shipping")
    pdf.body_text(
        "During peak seasons (November-December, Prime Day), Standard shipping may take an "
        "additional 2-3 business days. Express shipping timelines are maintained. We recommend "
        "ordering at least 10 days before any holiday deadline for Standard delivery."
    )

    return pdf


shipping_pdf = create_shipping_doc()
shipping_pdf.output(f"{VOLUME_PATH}/shipping_and_delivery_guidelines.pdf")
print("Created: shipping_and_delivery_guidelines.pdf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document 5: Customer FAQ & Troubleshooting Guide

# COMMAND ----------

def create_faq_doc():
    pdf = StyledPDF("Customer FAQ & Troubleshooting Guide", "TechCommerce Inc.")
    pdf.alias_nb_pages()
    pdf.add_title_page()

    pdf.add_page()
    pdf.section_header("1. Account & Membership")

    pdf.section_header("Q: What are the membership tiers and their benefits?", level=3)
    pdf.body_text(
        "TechCommerce offers four membership tiers based on annual spending:"
    )
    pdf.add_table(
        ["Tier", "Annual Spend", "Discount", "Key Benefits"],
        [
            ["Bronze", "$0 - $499", "0%", "Standard benefits"],
            ["Silver", "$500 - $1,999", "3%", "Free gift wrap, reduced shipping"],
            ["Gold", "$2,000 - $9,999", "5%", "Free standard shipping, priority support"],
            ["Platinum", "$10,000+", "10%", "Free express shipping, same-day dispatch, dedicated rep"],
        ],
        col_widths=[28, 38, 28, 96],
    )
    pdf.body_text(
        "Tier status is evaluated quarterly. If your spending drops below the threshold, you "
        "retain your current tier for one additional quarter as a grace period."
    )

    pdf.section_header("Q: How do I upgrade my membership tier?", level=3)
    pdf.body_text(
        "Membership tiers are upgraded automatically when your cumulative annual spending reaches "
        "the next tier threshold. Upgrades take effect immediately. You will receive an email "
        "notification and can see your new tier in your account dashboard."
    )

    pdf.section_header("Q: Can I combine membership discounts with promotional offers?", level=3)
    pdf.body_text(
        "Yes! Membership discounts stack with promotional offers. The membership discount is "
        "applied first, then any promotional discount is applied to the remaining amount. "
        "However, the maximum combined discount is capped at 25%."
    )

    pdf.section_header("2. Product Troubleshooting")

    pdf.section_header("Q: My CloudBuds left earbud is not working. What should I do?", level=3)
    pdf.body_text("Try these steps in order:")
    pdf.bullet_list([
        "1. Clean the charging contacts on the earbud and case with a dry cloth",
        "2. Place both earbuds in the case and charge for at least 30 minutes",
        "3. Factory reset: hold the case button for 10 seconds until LEDs flash red",
        "4. Re-pair: open case near your device and follow Bluetooth pairing steps",
        "5. If still not working, this is likely a hardware defect covered under the 1-year warranty",
    ])

    pdf.section_header("Q: My SmartFit Watch X battery drains in 4 hours. Is this normal?", level=3)
    pdf.body_text(
        "No. The SmartFit Watch X should last approximately 48 hours under typical use. "
        "Excessive battery drain may indicate:"
    )
    pdf.bullet_list([
        "Continuous GPS tracking mode is enabled (drains battery in ~8 hours)",
        "Always-on display brightness is set to maximum",
        "Background sync frequency is too high (set to every 30 minutes instead of real-time)",
        "Firmware may need updating -- check for updates in the SmartFit app",
        "If none of the above resolve the issue, the battery may be defective. "
        "Contact support for a warranty replacement within the 1-year warranty period.",
    ])

    pdf.section_header("Q: The ErgoDesk Step 7 assembly is confusing. Can you help?", level=3)
    pdf.body_text(
        "Step 7 involves attaching the horizontal cross-bar support beam to the two vertical "
        "leg columns. Common issues and solutions:"
    )
    pdf.bullet_list([
        "The bolt holes on the cross-bar must be aligned with the pre-drilled holes on the legs",
        "Ensure the cross-bar label 'TOP' faces upward",
        "Use the M8 bolts (silver, medium length) -- not the M6 bolts from the earlier steps",
        "Hand-tighten all 4 bolts first before using the Allen key to secure them",
        "If holes don't align, slightly loosen the bolts from Step 6 to allow adjustment",
    ])
    pdf.body_text(
        "[Figure: ErgoDesk cross-bar assembly diagram -- The cross-bar connects horizontally "
        "between the two vertical columns, approximately 15cm from the floor. Four M8 bolts "
        "secure it in place, two on each side.]"
    )

    pdf.section_header("Q: Can I upgrade the RAM on my UltraBook laptop?", level=3)
    pdf.body_text(
        "It depends on the model:\n"
        "- UltraBook Pro 15 (P001): YES. The RAM is user-upgradeable. It has one SODIMM slot "
        "accessible by removing the bottom panel (8 Phillips screws). Supports up to 32GB DDR5.\n"
        "- UltraBook Air 13 (P012): NO. The RAM is soldered to the motherboard and cannot be "
        "upgraded after purchase."
    )

    pdf.section_header("Q: Does the SoundBar Elite work with optical-only TVs?", level=3)
    pdf.body_text(
        "Yes! The SoundBar Elite supports both HDMI ARC/eARC and optical (Toslink) connections. "
        "If your TV only has an optical output, use the included optical cable. Note that optical "
        "connections support stereo and Dolby Digital 5.1 but NOT Dolby Atmos (which requires "
        "HDMI eARC). For the best surround sound experience with optical, select 'Dolby Digital' "
        "in your TV's audio output settings."
    )

    pdf.add_page()
    pdf.section_header("3. Orders & Payments")

    pdf.section_header("Q: What payment methods are accepted?", level=3)
    pdf.body_text("TechCommerce accepts the following payment methods:")
    pdf.bullet_list([
        "Credit Cards: Visa, Mastercard, American Express, Discover",
        "Debit Cards: Visa and Mastercard branded",
        "PayPal",
        "Apple Pay and Google Pay (mobile app only)",
        "TechCommerce Store Credit / Gift Cards",
    ])

    pdf.section_header("Q: Can I change or cancel my order after placing it?", level=3)
    pdf.body_text(
        "Orders can be modified or cancelled within 1 hour of placement if they haven't entered "
        "processing. After processing begins, cancellation triggers a standard return once "
        "delivered. Contact support immediately for urgent cancellation requests."
    )

    pdf.section_header("Q: How do I check the status of my order?", level=3)
    pdf.body_text("Order status values and their meanings:")
    pdf.add_table(
        ["Status", "Meaning", "Action Available"],
        [
            ["Processing", "Order received, preparing for shipment", "Cancel/modify"],
            ["Shipped", "Package handed to carrier", "Track shipment"],
            ["Delivered", "Carrier confirmed delivery", "Return/exchange"],
            ["Returned", "Return processed", "Track refund"],
            ["Cancelled", "Order cancelled before shipment", "Automatic refund"],
        ],
        col_widths=[35, 80, 75],
    )

    pdf.section_header("4. Privacy & Security")

    pdf.section_header("Q: How is my personal information protected?", level=3)
    pdf.body_text(
        "TechCommerce uses industry-standard AES-256 encryption for all stored personal data "
        "and TLS 1.3 for data in transit. We never share customer data with third parties for "
        "marketing purposes. Payment information is processed via PCI-DSS Level 1 certified "
        "payment processors and is never stored on our servers."
    )

    return pdf


faq_pdf = create_faq_doc()
faq_pdf.output(f"{VOLUME_PATH}/customer_faq_and_troubleshooting.pdf")
print("Created: customer_faq_and_troubleshooting.pdf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document 6: Membership Program Guide

# COMMAND ----------

def create_membership_doc():
    pdf = StyledPDF("TechCommerce Membership Program Guide", "Loyalty Rewards & Benefits")
    pdf.alias_nb_pages()
    pdf.add_title_page()

    pdf.add_page()
    pdf.section_header("1. Program Overview")
    pdf.body_text(
        "The TechCommerce Membership Program rewards loyal customers with increasing benefits "
        "based on annual spending. The program has four tiers: Bronze, Silver, Gold, and Platinum. "
        "All customers start at Bronze when they create an account."
    )

    pdf.section_header("2. Tier Qualification & Benefits")
    pdf.add_table(
        ["Benefit", "Bronze", "Silver", "Gold", "Platinum"],
        [
            ["Annual Spend Required", "$0", "$500", "$2,000", "$10,000"],
            ["Product Discount", "0%", "3%", "5%", "10%"],
            ["Standard Shipping", "$5.99*", "$3.99*", "Free", "Free"],
            ["Express Shipping", "$14.99", "$12.99", "$9.99", "Free"],
            ["Priority Support", "No", "No", "Yes", "Yes"],
            ["Dedicated Account Rep", "No", "No", "No", "Yes"],
            ["Early Access to Sales", "No", "No", "Yes", "Yes"],
            ["Free Gift Wrapping", "No", "Yes", "Yes", "Yes"],
            ["Birthday Discount", "No", "5%", "10%", "15%"],
            ["Extended Returns", "30 days", "30 days", "45 days", "60 days"],
            ["Same-Day Dispatch", "No", "No", "No", "Yes"],
        ],
        col_widths=[52, 30, 30, 30, 30],
    )
    pdf.body_text("* Free Standard shipping on orders over $50 (Bronze) or $35 (Silver).")

    pdf.section_header("3. Tier Evaluation & Grace Period")
    pdf.body_text(
        "Membership tiers are evaluated on a rolling 12-month basis at the end of each quarter. "
        "If your spending falls below the threshold for your current tier, you enter a grace period "
        "where you retain your current tier benefits for one additional quarter. This gives you "
        "time to reach the spending threshold again."
    )
    pdf.body_text(
        "Example: If you are Gold (requires $2,000/year) and your rolling 12-month spending is "
        "$1,800 at the end of Q2, you keep Gold benefits through Q3. If spending is still below "
        "$2,000 at the end of Q3, you will be downgraded to Silver."
    )

    pdf.section_header("4. Earning & Redeeming Points")
    pdf.body_text(
        "For every $1 spent (before discounts), you earn 1 TechPoint. Points can be redeemed "
        "for store credit at the following rates:"
    )
    pdf.add_table(
        ["Points", "Store Credit"],
        [
            ["500 points", "$5.00"],
            ["1,000 points", "$10.00"],
            ["2,500 points", "$27.50 (10% bonus)"],
            ["5,000 points", "$60.00 (20% bonus)"],
            ["10,000 points", "$130.00 (30% bonus)"],
        ],
        col_widths=[80, 80],
    )
    pdf.body_text(
        "Points expire 24 months after being earned. Points earned from returned items are "
        "deducted from your balance."
    )

    pdf.section_header("5. Platinum Member Exclusive Benefits")
    pdf.body_text("In addition to the standard tier benefits, Platinum members receive:")
    pdf.bullet_list([
        "Dedicated account representative reachable via direct phone line and email",
        "Same-day dispatch for all orders placed before 2 PM EST",
        "Free assembly service for furniture orders (normally $79.99)",
        "Annual appreciation gift (product value up to $100)",
        "Invitation to exclusive product preview events",
        "30-day price match guarantee -- if a product you purchased drops in price within 30 days, "
        "you receive the difference as store credit",
    ])

    pdf.section_header("6. Frequently Asked Questions")

    pdf.section_header("Q: Do returns affect my tier qualification?", level=3)
    pdf.body_text(
        "Yes. If you return a product, the purchase amount is deducted from your rolling 12-month "
        "spending total. This could potentially affect your tier if it brings your spending below "
        "the threshold."
    )

    pdf.section_header("Q: Can I combine my membership discount with a coupon code?", level=3)
    pdf.body_text(
        "Yes. Membership discounts are applied first, then coupon codes are applied to the "
        "remaining amount. The total combined discount is capped at 25%."
    )

    pdf.section_header("Q: What happens to my points if I downgrade tiers?", level=3)
    pdf.body_text(
        "Your points are retained regardless of tier changes. Points only expire based on the "
        "24-month expiration rule, not due to tier downgrade."
    )

    return pdf


membership_pdf = create_membership_doc()
membership_pdf.output(f"{VOLUME_PATH}/membership_program_guide.pdf")
print("Created: membership_program_guide.pdf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All PDFs Uploaded

# COMMAND ----------

import os

pdf_files = dbutils.fs.ls(f"dbfs:{VOLUME_PATH}")
print(f"\nPDFs in {VOLUME_PATH}:")
for f in pdf_files:
    print(f"  {f.name} ({f.size / 1024:.1f} KB)")
print(f"\nTotal: {len(pdf_files)} documents")
