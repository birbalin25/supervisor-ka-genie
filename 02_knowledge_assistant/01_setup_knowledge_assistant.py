# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Build Knowledge Assistant
# MAGIC
# MAGIC This notebook creates and configures a Knowledge Assistant that ingests the PDF documents
# MAGIC from the Unity Catalog Volume and provides a queryable endpoint for unstructured data.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run `01_create_delta_tables.py` (creates the catalog, schema, and volume)
# MAGIC - Run `02_generate_pdfs.py` (generates and uploads PDFs to the volume)
# MAGIC - Workspace must have serverless compute and Unity Catalog enabled
# MAGIC - The `databricks-gte-large-en` embedding model endpoint must be available
# MAGIC
# MAGIC **Reference:** https://docs.databricks.com/aws/en/generative-ai/agent-bricks/knowledge-assistant

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-import config after restart
CATALOG = "ka_genie_demo"
SCHEMA = "ecommerce"
VOLUME_NAME = "documents"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A: Create Knowledge Assistant via SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.knowledgeassistants import (
    KnowledgeAssistant,
    KnowledgeSource,
    FilesSpec,
)

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the Knowledge Assistant

# COMMAND ----------

# Create the Knowledge Assistant
ka = KnowledgeAssistant(
    display_name=KA_NAME,
    description=KA_DESCRIPTION,
    instructions=KA_INSTRUCTIONS,
)

created_ka = w.knowledge_assistants.create_knowledge_assistant(
    knowledge_assistant=ka
)

ka_id = created_ka.name.split("/")[-1]
print(f"Knowledge Assistant created!")
print(f"  Name: {created_ka.display_name}")
print(f"  ID: {ka_id}")
print(f"  Full resource name: {created_ka.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Add the UC Volume as a Knowledge Source
# MAGIC This tells the KA to ingest all PDFs from our volume.

# COMMAND ----------

files_source = KnowledgeSource(
    display_name="E-Commerce Policy Documents",
    description=(
        "PDF documents containing product specifications, return/refund policy, "
        "warranty terms, shipping guidelines, customer FAQ, troubleshooting guides, "
        "and membership program information for TechCommerce Inc."
    ),
    source_type="files",
    files=FilesSpec(path=VOLUME_PATH),
)

created_source = w.knowledge_assistants.create_knowledge_source(
    parent=f"knowledge-assistants/{ka_id}",
    knowledge_source=files_source,
)

source_id = created_source.name.split("/")[-1]
print(f"Knowledge Source added!")
print(f"  Source Name: {created_source.display_name}")
print(f"  Source ID: {source_id}")
print(f"  Path: {VOLUME_PATH}")
print(f"  Full resource name: {created_source.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Wait for indexing to complete
# MAGIC The Knowledge Assistant needs time to ingest and index the PDF documents.
# MAGIC This typically takes a few minutes depending on document size and count.

# COMMAND ----------

import time

print("Waiting for Knowledge Assistant to finish indexing documents...")
print("This may take several minutes. You can also check status in the Databricks UI.")
print(f"  Navigate to: Agents > Knowledge Assistant > {KA_NAME}")
print()

# The KA will be ready to query once indexing is complete.
# You can test it in the Databricks UI while waiting.
print("You can proceed to test the Knowledge Assistant in the UI while indexing completes.")
print("The Build tab > chat interface lets you send test queries.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Test the Knowledge Assistant
# MAGIC
# MAGIC Once the KA is ready, you can query it via the deployed endpoint.
# MAGIC The endpoint name follows the pattern: `knowledge-assistants-<ka_id>`

# COMMAND ----------

# Query the Knowledge Assistant endpoint
# After the KA is deployed, it creates a model serving endpoint

import requests
import json

# Get the workspace URL and token
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# The KA endpoint name
ka_endpoint_name = f"knowledge-assistants-{ka_id}"

def query_knowledge_assistant(question: str, ka_endpoint: str = ka_endpoint_name) -> str:
    """Query the Knowledge Assistant endpoint and return the response."""
    url = f"{workspace_url}/serving-endpoints/{ka_endpoint}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": [
            {"role": "user", "content": question}
        ]
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    result = response.json()

    # Extract the assistant's response
    if "choices" in result:
        return result["choices"][0]["message"]["content"]
    return str(result)


# Test queries
test_questions = [
    "What is the return policy for opened electronics?",
    "Can the UltraBook Pro 15 RAM be upgraded? How?",
    "What warranty does the ErgoDesk Standing Desk have?",
    "What troubleshooting steps should I try if my CloudBuds left earbud stops working?",
    "What are the benefits of Platinum membership?",
]

print("=" * 80)
print("KNOWLEDGE ASSISTANT TEST QUERIES")
print("=" * 80)

for i, q in enumerate(test_questions, 1):
    print(f"\n--- Test {i} ---")
    print(f"Q: {q}")
    try:
        answer = query_knowledge_assistant(q)
        print(f"A: {answer[:500]}...")
    except Exception as e:
        print(f"Error (KA may still be indexing): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option B: Create Knowledge Assistant via UI
# MAGIC
# MAGIC If you prefer to set up the Knowledge Assistant through the Databricks UI:
# MAGIC
# MAGIC 1. Navigate to **Agents** in the left sidebar
# MAGIC 2. Click **Knowledge Assistant** > **Build**
# MAGIC 3. Configure:
# MAGIC    - **Name:** E-Commerce Knowledge Base
# MAGIC    - **Description:** Answer questions about product specs, policies, and FAQs
# MAGIC    - **Knowledge Source Type:** UC Files
# MAGIC    - **Volume Path:** `/Volumes/ka_genie_demo/ecommerce/documents`
# MAGIC    - **Instructions:** "You are a helpful e-commerce assistant. Cite specific documents and sections."
# MAGIC 4. Click **Create**
# MAGIC 5. Test in the chat interface on the Build tab
# MAGIC 6. Note the Knowledge Assistant ID for use in the Supervisor Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save KA Configuration for Supervisor Agent

# COMMAND ----------

# Save the KA endpoint name for use by the Supervisor Agent
ka_config = {
    "ka_id": ka_id,
    "ka_endpoint_name": ka_endpoint_name,
    "ka_display_name": KA_NAME,
    "volume_path": VOLUME_PATH,
}

print("\n" + "=" * 80)
print("KNOWLEDGE ASSISTANT CONFIGURATION (save for Supervisor Agent)")
print("=" * 80)
for k, v in ka_config.items():
    print(f"  {k}: {v}")

# Store as a widget or spark config for downstream notebooks
spark.conf.set("ka_genie.ka_id", ka_id)
spark.conf.set("ka_genie.ka_endpoint_name", ka_endpoint_name)
