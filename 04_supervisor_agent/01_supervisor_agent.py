# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Supervisor Agent
# MAGIC
# MAGIC This notebook creates a Supervisor Agent that orchestrates both the Genie Space
# MAGIC (structured data) and the Knowledge Assistant (unstructured data) to answer
# MAGIC natural language queries using the best available source — or both.
# MAGIC
# MAGIC **Two approaches are provided:**
# MAGIC - **Option A:** Databricks Supervisor Agent (no-code, via SDK/UI) — recommended
# MAGIC - **Option B:** Custom Python Agent (full control over routing and synthesis)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Knowledge Assistant created and indexed (notebook 02)
# MAGIC - Genie Space created and tested (notebook 03)

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk openai mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-import after restart
CATALOG = "ka_genie_demo"
SCHEMA = "ecommerce"

# ============================================================
# Auto-discover Genie Space and KA endpoint
# ============================================================
import requests, json

_ws_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
_headers = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}

# Find Genie Space by name
GENIE_SPACE_ID = None
_spaces_resp = requests.get(f"{_ws_url}/api/2.0/genie/spaces", headers=_headers)
if _spaces_resp.status_code == 200:
    for s in _spaces_resp.json().get("spaces", []):
        if "e-commerce analytics" in (s.get("title") or "").lower():
            GENIE_SPACE_ID = s["space_id"]
            break
if not GENIE_SPACE_ID:
    GENIE_SPACE_ID = "01f133af9e081be2b3512622024e5fd7"  # fallback
print(f"Genie Space ID: {GENIE_SPACE_ID}")

# Find KA endpoint (most recent ka-*-endpoint created by current user)
KA_ENDPOINT_NAME = None
_ep_resp = requests.get(f"{_ws_url}/api/2.0/serving-endpoints", headers=_headers)
if _ep_resp.status_code == 200:
    _eps = _ep_resp.json().get("endpoints", [])
    _ka_eps = [e for e in _eps if e.get("name", "").startswith("ka-") and e.get("name", "").endswith("-endpoint")]
    _ka_eps.sort(key=lambda x: x.get("creation_timestamp", 0), reverse=True)
    if _ka_eps:
        KA_ENDPOINT_NAME = _ka_eps[0]["name"]
if not KA_ENDPOINT_NAME:
    KA_ENDPOINT_NAME = "ka-142c7f94-endpoint"  # fallback
print(f"KA Endpoint: {KA_ENDPOINT_NAME}")

KA_ID = KA_ENDPOINT_NAME.replace("ka-", "").replace("-endpoint", "")

SUPERVISOR_NAME = "E-Commerce Supervisor Agent"
SUPERVISOR_DESCRIPTION = (
    "A unified assistant that answers questions about e-commerce operations by "
    "combining structured data analytics (via Genie) with unstructured knowledge "
    "(via Knowledge Assistant). Routes queries intelligently to the right source."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Option A: Databricks Supervisor Agent (Recommended)
# MAGIC
# MAGIC Use the built-in Supervisor Agent feature to orchestrate Genie and KA.
# MAGIC This provides automatic routing, permissions management, and a deployed endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Supervisor Agent via SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Create the Supervisor via REST API
# MAGIC
# MAGIC The Supervisor Agent is configured through the Databricks Agents UI:
# MAGIC
# MAGIC 1. Navigate to **Agents** > **Supervisor Agent** > **Build**
# MAGIC 2. Name: `E-Commerce Supervisor Agent`
# MAGIC 3. Description: "Unified assistant combining structured analytics and document knowledge"
# MAGIC 4. **Add Agent 1 — Genie Space:**
# MAGIC    - Type: Genie Space
# MAGIC    - Select: `E-Commerce Analytics`
# MAGIC    - Description: "Query structured e-commerce data including customer demographics,
# MAGIC      product catalog, orders, order line items, and support tickets. Use for questions
# MAGIC      about sales metrics, customer behavior, order trends, revenue analytics, and
# MAGIC      support ticket statistics."
# MAGIC 5. **Add Agent 2 — Knowledge Assistant:**
# MAGIC    - Type: Agent Endpoint
# MAGIC    - Select: `knowledge-assistants-<ka_id>` endpoint
# MAGIC    - Description: "Answer questions about product specifications, return/refund policies,
# MAGIC      warranty terms and conditions, shipping guidelines, troubleshooting steps, membership
# MAGIC      program benefits, and other company documentation."
# MAGIC 6. **Instructions:**
# MAGIC    - "When a question requires both data lookup AND policy/documentation knowledge,
# MAGIC      query BOTH agents and combine the results into a single coherent answer.
# MAGIC      Always cite data sources: say 'According to our records...' for data
# MAGIC      and 'Per our policy documents...' for documentation."
# MAGIC 7. Click **Create Agent**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Programmatic setup via REST API (if Management APIs are available)

# COMMAND ----------

import requests
import json

workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

api_headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Option B: Custom Python Supervisor Agent
# MAGIC
# MAGIC For full control over routing logic and response synthesis, build a custom agent.
# MAGIC This agent uses an LLM to classify queries, calls the appropriate sub-agents,
# MAGIC and synthesizes a unified response.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Core Agent Implementation

# COMMAND ----------

import requests
import time
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class QueryRoute(Enum):
    """Possible routing destinations for a query."""
    GENIE_ONLY = "genie"
    KA_ONLY = "knowledge_assistant"
    BOTH = "both"


@dataclass
class SubAgentResponse:
    """Response from a sub-agent."""
    source: str
    content: str
    metadata: dict = field(default_factory=dict)
    success: bool = True
    error: Optional[str] = None


class GenieClient:
    """Client for querying the Genie Space via Conversation API."""

    def __init__(self, workspace_url: str, token: str, space_id: str):
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.space_id = space_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def query(self, question: str, max_wait: int = 120) -> SubAgentResponse:
        """Send a question to Genie and wait for the response."""
        try:
            # Start conversation
            start_resp = requests.post(
                f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/start-conversation",
                headers=self.headers,
                json={"content": question},
            )
            start_resp.raise_for_status()
            data = start_resp.json()
            conv_id = data["conversation"]["id"]
            msg_id = data["message"]["id"]

            # Poll for completion
            poll_interval = 2
            start_time = time.time()
            message = None

            while time.time() - start_time < max_wait:
                msg_resp = requests.get(
                    f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/"
                    f"conversations/{conv_id}/messages/{msg_id}",
                    headers=self.headers,
                )
                msg_resp.raise_for_status()
                message = msg_resp.json()
                status = message.get("status", "UNKNOWN")

                if status in ("COMPLETED", "FAILED", "CANCELLED"):
                    break
                time.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.5, 10)

            if message and message.get("status") == "COMPLETED":
                # Extract results
                text_parts = []
                sql_query = None
                query_data = None
                attachments = message.get("attachments", [])

                for att in attachments:
                    if att.get("text"):
                        text_parts.append(att["text"].get("content", ""))
                    if att.get("query"):
                        sql_query = att["query"].get("query", "")
                        att_id = att["attachment_id"]
                        try:
                            data_resp = requests.get(
                                f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/"
                                f"conversations/{conv_id}/messages/{msg_id}/"
                                f"attachments/{att_id}/query-result",
                                headers=self.headers,
                            )
                            if data_resp.status_code == 200:
                                query_data = data_resp.json()
                        except Exception:
                            pass

                # Format the response
                content = ""
                if text_parts:
                    content += "\n".join(text_parts)
                if sql_query:
                    content += f"\n\nSQL Query Used:\n```sql\n{sql_query}\n```"
                if query_data:
                    columns = query_data.get("manifest", {}).get("schema", {}).get("columns", [])
                    col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
                    rows = query_data.get("data_array", [])
                    if rows:
                        content += f"\n\nResults ({len(rows)} rows):\n"
                        content += " | ".join(col_names) + "\n"
                        content += "-" * (len(col_names) * 15) + "\n"
                        for row in rows[:20]:  # Limit display
                            content += " | ".join(str(v) for v in row) + "\n"
                        if len(rows) > 20:
                            content += f"... and {len(rows) - 20} more rows\n"

                return SubAgentResponse(
                    source="Genie (Structured Data)",
                    content=content or "Query completed but returned no results.",
                    metadata={"sql": sql_query, "row_count": len(rows) if query_data else 0},
                )
            else:
                return SubAgentResponse(
                    source="Genie (Structured Data)",
                    content="",
                    success=False,
                    error=f"Query ended with status: {message.get('status', 'UNKNOWN') if message else 'TIMEOUT'}",
                )

        except Exception as e:
            return SubAgentResponse(
                source="Genie (Structured Data)",
                content="",
                success=False,
                error=str(e),
            )


class KnowledgeAssistantClient:
    """Client for querying the Knowledge Assistant endpoint."""

    def __init__(self, workspace_url: str, token: str, endpoint_name: str):
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.endpoint_name = endpoint_name
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def query(self, question: str) -> SubAgentResponse:
        """Send a question to the Knowledge Assistant."""
        try:
            resp = requests.post(
                f"{self.workspace_url}/serving-endpoints/{self.endpoint_name}/invocations",
                headers=self.headers,
                json={
                    "messages": [{"role": "user", "content": question}]
                },
            )
            resp.raise_for_status()
            result = resp.json()

            content = ""
            if "choices" in result:
                content = result["choices"][0]["message"]["content"]
            else:
                content = str(result)

            return SubAgentResponse(
                source="Knowledge Assistant (Documents)",
                content=content,
                metadata={"endpoint": self.endpoint_name},
            )

        except Exception as e:
            return SubAgentResponse(
                source="Knowledge Assistant (Documents)",
                content="",
                success=False,
                error=str(e),
            )


class QueryRouter:
    """
    Routes queries to the appropriate sub-agent(s) using an LLM.

    The router analyzes the user's question and determines whether it should be
    handled by Genie (structured data), Knowledge Assistant (documents), or both.
    """

    ROUTING_PROMPT = """You are a query router for an e-commerce assistant system. Your job is to classify
user queries into one of three categories based on what data sources are needed.

Available data sources:
1. GENIE (Structured Data): Contains Delta tables with customers, products, orders, order_items, and
   support_tickets. Use for: analytics, metrics, counts, totals, trends, specific order/customer lookups,
   data aggregations, and any question answerable with SQL queries.

2. KNOWLEDGE_ASSISTANT (Documents): Contains PDFs about product specifications, return/refund policies,
   warranty terms, shipping guidelines, troubleshooting guides, membership program benefits, and FAQs.
   Use for: policy questions, product specs, how-to questions, troubleshooting, warranty details, and
   any question requiring knowledge from company documents.

3. BOTH: When the query requires information from both sources to give a complete answer. Examples:
   - "Is this customer's product under warranty?" (needs order date from Genie + warranty terms from docs)
   - "What's our return policy for the product that customer X ordered?" (needs order data + policy)
   - "How many tickets relate to warranty issues, and what does our warranty cover?" (needs data + docs)

Classify the following query. Respond with ONLY one word: GENIE, KNOWLEDGE_ASSISTANT, or BOTH.

Query: {query}

Classification:"""

    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def route(self, query: str) -> QueryRoute:
        """Classify a query and return the appropriate route."""
        try:
            # Use the Foundation Model API to classify the query
            resp = requests.post(
                f"{self.workspace_url}/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations",
                headers=self.headers,
                json={
                    "messages": [
                        {"role": "user", "content": self.ROUTING_PROMPT.format(query=query)}
                    ],
                    "max_tokens": 10,
                    "temperature": 0.0,
                },
            )
            resp.raise_for_status()
            result = resp.json()

            classification = result["choices"][0]["message"]["content"].strip().upper()

            if "BOTH" in classification:
                return QueryRoute.BOTH
            elif "KNOWLEDGE" in classification:
                return QueryRoute.KA_ONLY
            elif "GENIE" in classification:
                return QueryRoute.GENIE_ONLY
            else:
                # Default to BOTH if unclear
                return QueryRoute.BOTH

        except Exception as e:
            print(f"Router error (defaulting to BOTH): {e}")
            return QueryRoute.BOTH


class ResponseSynthesizer:
    """Synthesizes responses from multiple sub-agents into a coherent answer."""

    SYNTHESIS_PROMPT = """You are a helpful e-commerce assistant. You have received responses from two
data sources about a customer's question. Combine them into a single, coherent, and helpful answer.

Rules:
- If both sources provided information, integrate them naturally
- When citing data, say "According to our records..." or "Our data shows..."
- When citing documents/policies, say "Per our documentation..." or "Our policy states..."
- If one source had an error, use the successful source and note the limitation
- Be concise but thorough
- Use bullet points for lists
- Include specific numbers and data when available

User Question: {question}

Structured Data Response (from Genie):
{genie_response}

Document Knowledge Response (from Knowledge Assistant):
{ka_response}

Synthesized Answer:"""

    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def synthesize(
        self,
        question: str,
        genie_response: Optional[SubAgentResponse],
        ka_response: Optional[SubAgentResponse],
    ) -> str:
        """Combine sub-agent responses into a unified answer."""

        # If only one source, return it directly
        if genie_response and not ka_response:
            if genie_response.success:
                return f"**[Source: Structured Data]**\n\n{genie_response.content}"
            return f"Unable to retrieve structured data: {genie_response.error}"

        if ka_response and not genie_response:
            if ka_response.success:
                return f"**[Source: Documentation]**\n\n{ka_response.content}"
            return f"Unable to retrieve documentation: {ka_response.error}"

        # Both sources — synthesize
        genie_text = genie_response.content if genie_response and genie_response.success else f"[Error: {genie_response.error}]"
        ka_text = ka_response.content if ka_response and ka_response.success else f"[Error: {ka_response.error}]"

        try:
            resp = requests.post(
                f"{self.workspace_url}/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations",
                headers=self.headers,
                json={
                    "messages": [
                        {
                            "role": "user",
                            "content": self.SYNTHESIS_PROMPT.format(
                                question=question,
                                genie_response=genie_text,
                                ka_response=ka_text,
                            ),
                        }
                    ],
                    "max_tokens": 1000,
                    "temperature": 0.1,
                },
            )
            resp.raise_for_status()
            result = resp.json()
            return result["choices"][0]["message"]["content"]

        except Exception as e:
            # Fallback: concatenate responses
            parts = []
            if genie_response and genie_response.success:
                parts.append(f"**From Structured Data:**\n{genie_response.content}")
            if ka_response and ka_response.success:
                parts.append(f"**From Documentation:**\n{ka_response.content}")
            return "\n\n---\n\n".join(parts) if parts else f"Error synthesizing response: {e}"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Supervisor Agent Main Class

# COMMAND ----------

class SupervisorAgent:
    """
    The Supervisor Agent orchestrates Genie and Knowledge Assistant to answer
    user queries about e-commerce data and policies.

    Flow:
    1. Receive natural language query
    2. Route to appropriate sub-agent(s) using LLM classification
    3. Execute sub-agent queries in parallel (when both needed)
    4. Synthesize results into a unified response
    """

    def __init__(self, workspace_url: str, token: str, genie_space_id: str, ka_endpoint_name: str):
        self.genie = GenieClient(workspace_url, token, genie_space_id)
        self.ka = KnowledgeAssistantClient(workspace_url, token, ka_endpoint_name)
        self.router = QueryRouter(workspace_url, token)
        self.synthesizer = ResponseSynthesizer(workspace_url, token)
        self.conversation_history = []

    def ask(self, question: str, verbose: bool = True) -> str:
        """
        Process a user question through the supervisor pipeline.

        Args:
            question: Natural language question from the user
            verbose: If True, print routing and progress information

        Returns:
            Synthesized answer string
        """
        if verbose:
            print(f"\n{'='*80}")
            print(f"USER QUERY: {question}")
            print(f"{'='*80}")

        # Step 1: Route the query
        route = self.router.route(question)
        if verbose:
            print(f"\n[Router] Decision: {route.value}")

        # Step 2: Execute sub-agent queries
        genie_response = None
        ka_response = None

        if route in (QueryRoute.GENIE_ONLY, QueryRoute.BOTH):
            if verbose:
                print("[Genie] Querying structured data...")
            genie_response = self.genie.query(question)
            if verbose:
                status = "Success" if genie_response.success else f"Error: {genie_response.error}"
                print(f"[Genie] {status}")

        if route in (QueryRoute.KA_ONLY, QueryRoute.BOTH):
            if verbose:
                print("[KA] Querying knowledge base...")
            ka_response = self.ka.query(question)
            if verbose:
                status = "Success" if ka_response.success else f"Error: {ka_response.error}"
                print(f"[KA] {status}")

        # Step 3: Synthesize response
        if verbose:
            print("[Synthesizer] Combining responses...")

        answer = self.synthesizer.synthesize(question, genie_response, ka_response)

        # Track conversation history
        self.conversation_history.append({
            "question": question,
            "route": route.value,
            "genie_success": genie_response.success if genie_response else None,
            "ka_success": ka_response.success if ka_response else None,
        })

        if verbose:
            print(f"\n{'='*80}")
            print("ANSWER:")
            print(f"{'='*80}")
            print(answer)

        return answer

    def get_history(self) -> list:
        """Return conversation history."""
        return self.conversation_history


# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize and Test the Supervisor Agent

# COMMAND ----------

# Initialize the Supervisor Agent
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

supervisor = SupervisorAgent(
    workspace_url=workspace_url,
    token=token,
    genie_space_id=GENIE_SPACE_ID,
    ka_endpoint_name=KA_ENDPOINT_NAME,
)

print("Supervisor Agent initialized!")
print(f"  Genie Space: {GENIE_SPACE_ID}")
print(f"  KA Endpoint: {KA_ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Test Queries

# COMMAND ----------

# Test Query 1: Simple Lookup + Policy (BOTH)
answer = supervisor.ask(
    "Customer C008 returned their CloudBuds (order ORD-1008). "
    "What is the return window for electronics, and will they be charged a restocking fee?"
)

# COMMAND ----------

# Test Query 2: Product Spec + Data (BOTH)
answer = supervisor.ask(
    "Can the UltraBook Pro 15 RAM be upgraded? Also, how many customers have purchased this laptop?"
)

# COMMAND ----------

# Test Query 3: Warranty Reasoning (BOTH)
answer = supervisor.ask(
    "Clara Davis has an open support ticket about her UltraBook Pro screen flickering. "
    "Is this covered under warranty, and what are her options?"
)

# COMMAND ----------

# Test Query 4: Genie Only
answer = supervisor.ask(
    "What is the total revenue by product category for delivered orders?"
)

# COMMAND ----------

# Test Query 5: KA Only
answer = supervisor.ask(
    "What troubleshooting steps should I follow if my SmartFit Watch battery drains too quickly?"
)

# COMMAND ----------

# Test Query 6: Complex Multi-Step (BOTH)
answer = supervisor.ask(
    "Give me a complete customer profile for Grace Chen — her orders, support history, "
    "membership benefits, and any relevant product details or policy information."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Conversation History

# COMMAND ----------

import pandas as pd

history = supervisor.get_history()
history_df = pd.DataFrame(history)
display(spark.createDataFrame(history_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Option C: Deploy as a Databricks App (Production)
# MAGIC
# MAGIC For production use, wrap the Supervisor Agent as a Databricks App with a REST API.

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLflow Model Wrapper

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel


class SupervisorAgentModel(PythonModel):
    """MLflow-compatible wrapper for the Supervisor Agent."""

    def load_context(self, context):
        import os
        self.workspace_url = os.environ.get("DATABRICKS_HOST", "")
        self.token = os.environ.get("DATABRICKS_TOKEN", "")
        self.genie_space_id = os.environ.get("GENIE_SPACE_ID", "")
        self.ka_endpoint_name = os.environ.get("KA_ENDPOINT_NAME", "")

        self.agent = SupervisorAgent(
            workspace_url=self.workspace_url,
            token=self.token,
            genie_space_id=self.genie_space_id,
            ka_endpoint_name=self.ka_endpoint_name,
        )

    def predict(self, context, model_input):
        """Process a batch of questions."""
        if isinstance(model_input, pd.DataFrame):
            questions = model_input["question"].tolist()
        elif isinstance(model_input, dict):
            questions = [model_input.get("question", "")]
        else:
            questions = [str(model_input)]

        results = []
        for q in questions:
            answer = self.agent.ask(q, verbose=False)
            results.append({"question": q, "answer": answer})
        return results


# Log the model to MLflow
# Uncomment below to register:
#
# with mlflow.start_run(run_name="supervisor_agent"):
#     mlflow.pyfunc.log_model(
#         artifact_path="supervisor_agent",
#         python_model=SupervisorAgentModel(),
#         registered_model_name=f"{CATALOG}.{SCHEMA}.supervisor_agent",
#         pip_requirements=["databricks-sdk", "requests", "pandas"],
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Architecture Summary
# MAGIC
# MAGIC ```
# MAGIC                    ┌─────────────────────┐
# MAGIC                    │   User Query         │
# MAGIC                    │  (Natural Language)   │
# MAGIC                    └──────────┬────────────┘
# MAGIC                               │
# MAGIC                    ┌──────────▼────────────┐
# MAGIC                    │   SUPERVISOR AGENT     │
# MAGIC                    │                        │
# MAGIC                    │  ┌──────────────────┐  │
# MAGIC                    │  │  Query Router     │  │
# MAGIC                    │  │  (LLM-based)      │  │
# MAGIC                    │  └──────┬───────────┘  │
# MAGIC                    │         │               │
# MAGIC                    │    ┌────┴────┐          │
# MAGIC                    │    │         │          │
# MAGIC                    │    ▼         ▼          │
# MAGIC           ┌────────┤  Genie    Knowledge    │
# MAGIC           │        │  Client   Assistant    │
# MAGIC           │        │           Client       │
# MAGIC           │        │    │         │          │
# MAGIC           │        │    └────┬────┘          │
# MAGIC           │        │         │               │
# MAGIC           │        │  ┌──────▼───────────┐  │
# MAGIC           │        │  │ Response          │  │
# MAGIC           │        │  │ Synthesizer       │  │
# MAGIC           │        │  │ (LLM-based)       │  │
# MAGIC           │        │  └──────────────────┘  │
# MAGIC           │        └────────────────────────┘
# MAGIC           │
# MAGIC     ┌─────┴──────────────────────────────────┐
# MAGIC     │                                          │
# MAGIC     ▼                                          ▼
# MAGIC ┌───────────────┐                  ┌───────────────────┐
# MAGIC │  GENIE SPACE   │                  │ KNOWLEDGE ASSISTANT│
# MAGIC │                │                  │                    │
# MAGIC │ Delta Tables:  │                  │ PDF Documents:     │
# MAGIC │ • customers    │                  │ • Product Catalog  │
# MAGIC │ • products     │                  │ • Return Policy    │
# MAGIC │ • orders       │                  │ • Warranty Terms   │
# MAGIC │ • order_items  │                  │ • Shipping Guide   │
# MAGIC │ • support_tix  │                  │ • FAQ/Troubleshoot │
# MAGIC │                │                  │ • Membership Guide │
# MAGIC └───────────────┘                  └───────────────────┘
# MAGIC ```
