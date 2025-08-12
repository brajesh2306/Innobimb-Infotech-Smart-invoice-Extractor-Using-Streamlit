# app.py
"""
Streamlit Dashboard - PO vs Invoice Reconciler (Excel history, Azure backend creds only)
Save this file and run: streamlit run app.py
"""

import io
import os
import re
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
import streamlit as st
from fuzzywuzzy import fuzz

# Azure SDK
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient

# ---------------------------
# CONFIG - put your Azure creds here (backend-only)
# ---------------------------
AZURE_ENDPOINT = "https://innobimbinfotechdocument.cognitiveservices.azure.com/"
AZURE_KEY = "BPYdCPFIWRnZg2JScpaL7LAIJuPiJPYWPTSx0ABzgXB99aDSc1AvJQQJ99BHACYeBjFXJ3w3AAALACOGa6Ie"

# History file (local Excel)
HISTORY_FILE = "comparison_history.xlsx"

# Matching params
FUZZY_THRESHOLD = 86
WORD_OVERLAP_MIN = 1  # at least this many overlapping words to accept name-match

# ---------------------------
# Utilities
# ---------------------------
def normalize_sku_to_model(text: str) -> str:
    """Normalize SKU/description to core ModelCode like MX-AV425."""
    if not isinstance(text, str):
        return None
    s = text.upper().strip()
    s = re.sub(r"^PA[-_]*", "", s)  # remove PA- prefix
    # remove color/suffix tokens
    s = re.sub(r"\b(CB|BLK|BLUE|BLC|GRN|WHT|RED|CHARCOAL|BLACK|SIL|BR|BK|NOS|PCS)\b", " ", s)
    s = re.sub(r"[^A-Z0-9\- ]", " ", s)
    m = re.search(r"MX[-\s]*AV[-\s]*?(\d{2,4})", s)
    if m:
        return f"MX-AV{m.group(1)}"
    m2 = re.search(r"MX-AV\d{2,4}", s)
    if m2:
        return m2.group(0)
    return None

def clean_number(x) -> float:
    try:
        if pd.isna(x):
            return 0.0
        s = str(x)
        s = re.sub(r"[^\d\.\-]", "", s.replace(",", ""))
        return float(s) if s != "" else 0.0
    except Exception:
        return 0.0

def fuzzy_same(a: str, b: str, thresh: int = FUZZY_THRESHOLD) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    return fuzz.partial_ratio(str(a), str(b)) >= thresh

def words_overlap(a: str, b: str) -> int:
    if not a or not b:
        return 0
    wa = {w for w in re.split(r"\W+", a.upper()) if len(w) >= 2}
    wb = {w for w in re.split(r"\W+", b.upper()) if len(w) >= 2}
    return len(wa & wb)

# ---------------------------
# PO Excel reading
# ---------------------------
def detect_header_row_from_bytes(excel_bytes: bytes) -> int:
    raw = pd.read_excel(io.BytesIO(excel_bytes), header=None, engine="openpyxl")
    for idx, row in raw.iterrows():
        combined = " ".join([str(x) for x in row.values if pd.notna(x)])
        if "SKU" in combined.upper() or "PRODUCT" in combined.upper() or "NAME" in combined.upper():
            return idx
    return 0

def read_po_excel_bytes(excel_bytes: bytes) -> pd.DataFrame:
    header_row = detect_header_row_from_bytes(excel_bytes)
    df = pd.read_excel(io.BytesIO(excel_bytes), header=header_row, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    def find_col(keywords):
        for c in df.columns:
            cu = str(c).upper()
            if any(k in cu for k in keywords):
                return c
        return None

    sku_col = find_col(["SKU", "PRODUCT CODE", "ITEM CODE", "ITEM"])
    name_col = find_col(["NAME", "DESCRIPTION", "PRODUCT"])
    po_ref_col = find_col(["PO REF", "PO_REF", "PO NUMBER", "PO NO", "PO"])
    ordered_col = find_col(["ORDER", "ORDERED", "QTY", "QUANTITY", "ORDERED QUANTITY"])
    base_price_col = find_col(["BASE PRICE", "BASE_PRICE", "COST", "MRP"])
    item_price_col = find_col(["ITEM PRICE", "ITEM_PRICE", "UNIT PRICE", "PRICE"])
    tax_col = find_col(["TAX", "INCLUDED TAX", "TAX RATE"])
    total_col = find_col(["TOTAL VALUE", "TOTAL", "AMOUNT"])

    po = pd.DataFrame()
    po["SKU"] = df[sku_col] if sku_col and sku_col in df.columns else df.iloc[:, 0]
    po["Name"] = df[name_col] if name_col and name_col in df.columns else po["SKU"].astype(str)
    po["PO Ref No"] = df[po_ref_col] if po_ref_col and po_ref_col in df.columns else ""
    po["Ordered Quantity"] = df[ordered_col].apply(clean_number) if ordered_col and ordered_col in df.columns else 0.0
    po["Base Price"] = df[base_price_col].apply(clean_number) if base_price_col and base_price_col in df.columns else 0.0
    po["Item Price"] = df[item_price_col].apply(clean_number) if item_price_col and item_price_col in df.columns else 0.0
    po["Included Tax"] = df[tax_col].apply(clean_number) if tax_col and tax_col in df.columns else 0.0
    po["Total Value"] = df[total_col].apply(clean_number) if total_col and total_col in df.columns else 0.0

    po["ModelCode"] = po["SKU"].astype(str).apply(normalize_sku_to_model)
    po["PO Ref No"] = po["PO Ref No"].astype(str).str.strip()
    return po

# ---------------------------
# Azure invoice extraction
# ---------------------------
def analyze_invoice_bytes(invoice_bytes: bytes) -> Dict[str, Any]:
    endpoint = AZURE_ENDPOINT or os.getenv("AZURE_ENDPOINT")
    key = AZURE_KEY or os.getenv("AZURE_KEY")
    if not endpoint or not key or "<your" in str(endpoint) or "<your" in str(key):
        raise ValueError("Azure endpoint/key not configured. Please set AZURE_ENDPOINT and AZURE_KEY in this file or as env vars.")
    client = DocumentAnalysisClient(endpoint, AzureKeyCredential(key))
    poller = client.begin_analyze_document("prebuilt-invoice", invoice_bytes)
    result = poller.result()

    out = {"fields": {}, "tables": [], "raw_text": [], "line_items": [], "normalized_line_items": []}

    for doc in result.documents:
        for name, field in doc.fields.items():
            try:
                val = field.value if hasattr(field, "value") else field.content
            except Exception:
                val = getattr(field, "content", None)
            out["fields"][name] = val

    for table in result.tables:
        grid = [["" for _ in range(table.column_count)] for _ in range(table.row_count)]
        for cell in table.cells:
            grid[cell.row_index][cell.column_index] = (cell.content or "").strip()
        out["tables"].append(grid)
        if table.row_count >= 2:
            headers = [h.strip() for h in grid[0]]
            for row in grid[1:]:
                rowd = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
                out["line_items"].append(rowd)

    for page in result.pages:
        if hasattr(page, "lines"):
            for line in page.lines:
                out["raw_text"].append(line.content)

    detected_po = None
    possible_keys = ["PO", "PONumber", "PO Number", "PurchaseOrderNumber", "PurchaseOrder"]
    for k in possible_keys:
        if out["fields"].get(k):
            detected_po = out["fields"].get(k)
            break
    if not detected_po:
        for line in out["raw_text"]:
            m = re.search(r"(FBA[A-Z0-9]{5,})", str(line).upper())
            if m:
                detected_po = m.group(1)
                break
    out["fields"]["DetectedPO"] = detected_po

    norm = []
    for item in out["line_items"]:
        desc_key = next((k for k in item.keys() if "desc" in k.lower() or "product" in k.lower()), None)
        qty_key = next((k for k in item.keys() if k.lower() in ("qty", "quantity", "no", "nos")), None)
        rate_key = next((k for k in item.keys() if "rate" in k.lower() or "unit price" in k.lower() or "unitprice" in k.lower()), None)
        amt_key = next((k for k in item.keys() if "amount" in k.lower() or "qty*rate" in k.lower() or "total" in k.lower()), None)
        code_key = next((k for k in item.keys() if "code" in k.lower() or "hsn" in k.lower()), None)

        desc = item.get(desc_key) if desc_key else " ".join([str(v) for v in item.values() if v])
        qty = clean_number(item.get(qty_key, 0)) if qty_key else 0.0
        rate = clean_number(item.get(rate_key, 0)) if rate_key else 0.0
        amt = clean_number(item.get(amt_key, rate * qty)) if amt_key else rate * qty
        code = item.get(code_key) if code_key else None

        model = normalize_sku_to_model(f"{desc} {code or ''}")
        norm.append({
            "Description": desc,
            "Quantity": qty,
            "UnitPrice": rate,
            "Amount": amt,
            "Code": code,
            "ModelCode": model,
            "PO Ref No": detected_po
        })

    out["normalized_line_items"] = norm
    return out

# ---------------------------
# History operations (Excel)
# ---------------------------
def load_history_df() -> pd.DataFrame:
    if os.path.exists(HISTORY_FILE):
        try:
            return pd.read_excel(HISTORY_FILE, engine="openpyxl")
        except Exception:
            return pd.DataFrame(columns=[
                "PO Ref No", "SKU", "ModelCode", "Ordered Quantity", "Cumulative Delivered",
                "Pending Quantity", "Base Price", "Item Price", "Included Tax", "Total Value", "Status", "Last Updated"
            ])
    else:
        return pd.DataFrame(columns=[
            "PO Ref No", "SKU", "ModelCode", "Ordered Quantity", "Cumulative Delivered",
            "Pending Quantity", "Base Price", "Item Price", "Included Tax", "Total Value", "Status", "Last Updated"
        ])

def save_history_df(df: pd.DataFrame):
    if "Last Updated" not in df.columns:
        df["Last Updated"] = datetime.utcnow().isoformat()
    df.to_excel(HISTORY_FILE, index=False)

# ---------------------------
# Reconciliation core
# ---------------------------
def reconcile(po_df: pd.DataFrame, invoice_parsed: Dict[str, Any]) -> (pd.DataFrame, pd.DataFrame):
    history = load_history_df()
    inv_items = pd.DataFrame(invoice_parsed.get("normalized_line_items", []))
    if inv_items.empty:
        inv_items = pd.DataFrame(columns=["Description", "Quantity", "UnitPrice", "Amount", "Code", "ModelCode", "PO Ref No"])

    results: List[Dict[str, Any]] = []
    def prev_cumulative(po_ref: str, model: str) -> float:
        if history.empty:
            return 0.0
        mask = (history["PO Ref No"].astype(str) == str(po_ref)) & (history["ModelCode"].astype(str) == str(model))
        if mask.any():
            return float(history[mask].iloc[0]["Cumulative Delivered"] or 0.0)
        return 0.0

    po_models = set(po_df["ModelCode"].dropna().astype(str))

    for _, prow in po_df.iterrows():
        po_ref = prow.get("PO Ref No") or invoice_parsed["fields"].get("DetectedPO") or ""
        model = prow.get("ModelCode")
        sku = prow.get("SKU")
        name = prow.get("Name")
        ordered = float(prow.get("Ordered Quantity") or 0)
        base_price_po = float(prow.get("Base Price") or 0)
        item_price_po = float(prow.get("Item Price") or 0)
        included_tax_po = float(prow.get("Included Tax") or 0)
        total_value_po = float(prow.get("Total Value") or 0)

        inv_candidates = inv_items.copy()
        inv_detected_po = invoice_parsed["fields"].get("DetectedPO")
        if inv_detected_po:
            inv_candidates = inv_candidates[inv_candidates["PO Ref No"].astype(str).str.strip() == str(inv_detected_po).strip()]

        matched = inv_candidates[inv_candidates["ModelCode"].astype(str) == str(model)] if not inv_candidates.empty else pd.DataFrame()
        if (matched is None or matched.empty) and model:
            matched = inv_candidates[inv_candidates["ModelCode"].apply(lambda x: fuzzy_same(str(x), str(model)) if pd.notna(x) else False)]
        if (matched is None or matched.empty):
            def desc_match(row):
                desc = str(row.get("Description", ""))
                return words_overlap(str(name), desc) >= WORD_OVERLAP_MIN or fuzzy_same(str(name), desc)
            matched = inv_candidates[inv_candidates.apply(desc_match, axis=1)] if not inv_candidates.empty else pd.DataFrame()

        delivered_now = float(matched["Quantity"].sum()) if not matched.empty else 0.0
        delivered_value_now = float((matched["Quantity"] * matched["UnitPrice"]).sum()) if not matched.empty else 0.0

        prev_cum = prev_cumulative(po_ref, model)
        cumulative = prev_cum + delivered_now
        pending = ordered - cumulative

        if delivered_now == 0 and cumulative == 0:
            status = "Not Delivered"
        elif pending == 0:
            status = "Completed"
        elif pending > 0:
            status = "Pending"
        else:
            status = "Over Delivered"

        inv_unit_price = None
        if not matched.empty:
            total_qty = matched["Quantity"].sum()
            if total_qty > 0:
                inv_unit_price = (matched["Quantity"] * matched["UnitPrice"]).sum() / total_qty

        price_match = None
        if inv_unit_price is not None:
            price_match = abs(inv_unit_price - base_price_po) < 0.01

        total_match = None
        if delivered_value_now and total_value_po:
            total_match = abs(delivered_value_now - total_value_po) < 0.01

        product_matched = not (matched is None or matched.empty)
        name_similarity = words_overlap(str(name), " ".join(matched["Description"].astype(str))) if product_matched else 0

        results.append({
            "PO Ref No": po_ref,
            "SKU": sku,
            "Name (PO)": name,
            "ModelCode": model,
            "Ordered Quantity": ordered,
            "Delivered (This Invoice)": delivered_now,
            "Delivered Value (This Invoice)": delivered_value_now,
            "Previous Cumulative Delivered": prev_cum,
            "Cumulative Delivered": cumulative,
            "Pending Quantity": pending,
            "Status": status,
            "Base Price (PO)": base_price_po,
            "Unit Price (Inv avg)": inv_unit_price,
            "Base Price Match": price_match,
            "Total Value (PO)": total_value_po,
            "Total Match": total_match,
            "Product Matched": product_matched,
            "Name Overlap Words": name_similarity
        })

    # invoice-only items
    for _, inv in inv_items.iterrows():
        m = inv.get("ModelCode")
        if not m or str(m) in po_models:
            continue
        qty = float(inv.get("Quantity", 0) or 0)
        unit_price = float(inv.get("UnitPrice", 0) or 0)
        amt = float(inv.get("Amount", 0) or 0)
        results.append({
            "PO Ref No": inv.get("PO Ref No") or "",
            "SKU": None,
            "Name (PO)": None,
            "ModelCode": m,
            "Ordered Quantity": 0,
            "Delivered (This Invoice)": qty,
            "Delivered Value (This Invoice)": qty * unit_price,
            "Previous Cumulative Delivered": 0.0,
            "Cumulative Delivered": qty,
            "Pending Quantity": 0,
            "Status": "Not Ordered",
            "Base Price (PO)": None,
            "Unit Price (Inv avg)": unit_price,
            "Base Price Match": None,
            "Total Value (PO)": amt,
            "Total Match": None,
            "Product Matched": False,
            "Name Overlap Words": 0
        })

    result_df = pd.DataFrame(results)

    # upsert into history
    hist = history.copy()
    now_iso = datetime.utcnow().isoformat()
    for _, r in result_df.iterrows():
        po_ref = r["PO Ref No"]
        model = r["ModelCode"]
        mask = (hist["PO Ref No"].astype(str) == str(po_ref)) & (hist["ModelCode"].astype(str) == str(model))
        row_dict = {
            "PO Ref No": po_ref,
            "SKU": r.get("SKU"),
            "ModelCode": model,
            "Ordered Quantity": r.get("Ordered Quantity"),
            "Cumulative Delivered": r.get("Cumulative Delivered"),
            "Pending Quantity": r.get("Pending Quantity"),
            "Base Price": r.get("Base Price (PO)"),
            "Item Price": r.get("Unit Price (Inv avg)"),
            "Included Tax": None,
            "Total Value": r.get("Total Value (PO)"),
            "Status": r.get("Status"),
            "Last Updated": now_iso
        }
        if not hist.empty and mask.any():
            idx = hist[mask].index[0]
            for k, v in row_dict.items():
                hist.loc[idx, k] = v
        else:
            hist = pd.concat([hist, pd.DataFrame([row_dict])], ignore_index=True)

    # save history
    try:
        save_history_df(hist)
    except Exception as e:
        st.warning(f"Failed to save history file: {e}")

    return result_df, hist

# ---------------------------
# Streamlit UI - Dashboard
# ---------------------------
st.set_page_config(page_title="PO vs Invoice Dashboard", layout="wide")
st.title("📦 PO vs Invoice Reconciler ")

st.markdown("Upload Purchase Order Excel and Invoice PDF. Azure keys must be set in the script (top constants).")

col_upload, col_dummy = st.columns([3, 1])
with col_upload:
    uploaded_po = st.file_uploader("Upload Purchase Order Excel (.xls/.xlsx)", type=["xls", "xlsx"])
    uploaded_invoice = st.file_uploader("Upload Invoice PDF (.pdf)", type=["pdf"])
    run = st.button("Run Reconciliation")

with col_dummy:
    if st.button("Show current history"):
        history_display = load_history_df()
        if history_display.empty:
            st.info("No history yet.")
        else:
            st.dataframe(history_display)

if run:
    if not uploaded_po or not uploaded_invoice:
        st.error("Please upload both PO Excel and Invoice PDF.")
    else:
        try:
            po_bytes = uploaded_po.read()
            po_df = read_po_excel_bytes(po_bytes)
            st.success("PO parsed.")
        except Exception as e:
            st.error(f"Failed reading PO Excel: {e}")
            raise

        try:
            inv_bytes = uploaded_invoice.read()
            parsed = analyze_invoice_bytes(inv_bytes)
            st.success("Invoice extracted.")
        except Exception as e:
            st.error(f"Invoice extraction failed: {e}")
            raise

        # run reconciliation
        with st.spinner("Reconciling..."):
            result_df, history_df = reconcile(po_df, parsed)

        # KPIs
        total_lines = len(result_df)
        completed = (result_df["Status"] == "Completed").sum()
        pending = (result_df["Status"] == "Pending").sum()
        over = (result_df["Status"] == "Over Delivered").sum()
        not_ordered = (result_df["Status"] == "Not Ordered").sum()
        invoice_total = parsed["fields"].get("InvoiceTotal") or parsed["fields"].get("Invoice Total") or parsed["fields"].get("InvoiceTotalAmount") or parsed["fields"].get("InvoiceAmount") or None

        st.subheader("Summary KPIs")
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("PO lines (this run)", total_lines)
        k2.metric("Completed", int(completed))
        k3.metric("Pending", int(pending))
        k4.metric("Over Delivered", int(over))
        k5.metric("Not Ordered", int(not_ordered))

        if invoice_total:
            st.write(f"**Invoice total (extracted):** {invoice_total}")

        st.subheader("Comparison (this run)")
        st.dataframe(result_df)

        towrite = io.BytesIO()
        result_df.to_excel(towrite, index=False, engine="openpyxl")
        towrite.seek(0)
        st.download_button("Download This Run Result (xlsx)", data=towrite.getvalue(), file_name="reconciliation_result.xlsx")

        st.subheader("Cumulative History")
        st.dataframe(history_df)
        towrite2 = io.BytesIO()
        history_df.to_excel(towrite2, index=False, engine="openpyxl")
        towrite2.seek(0)
        st.download_button("Download History (xlsx)", data=towrite2.getvalue(), file_name=HISTORY_FILE)

st.markdown("### Notes")
st.markdown("""
- Azure credentials are backend-only: put values at top of this file (AZURE_ENDPOINT & AZURE_KEY) or set env vars AZURE_ENDPOINT / AZURE_KEY.
- Matching flow: **PO Ref No (if present)** -> **ModelCode** -> **fuzzy model** -> **product name overlap/fuzzy**.
- Business rule: `Pending = Ordered - CumulativeDelivered`. `0` → Completed, `>0` → Pending, `<0` → Over Delivered (problem).
- History persisted in `comparison_history.xlsx`. For production use a DB (MySQL/Postgres).
""")
