# Databricks notebook source
# MAGIC %run ./nb_utils

# COMMAND ----------

# Silver step 1: one row per bill and field, typed, with the AI's confidence.
CUPS_PATTERN = re.compile(r"^ES\d{16}[A-Z]{2}([A-Z]{2}|\d[A-Z])?$")   # ES + 16 digits + 2 letters (+ 2 letters, or 1 digit and 1 letter)
REQUIRED = ["InvoiceNumber", "CUPS", "BillingStartDate", "BillingEndDate", "TotalAmount"]

bronze = spark.read.table("bronze_extractions").where("status = 'Succeeded'").collect()
field_rows, bills = [], []
for row in bronze:
    found = ai_fields(row.extracted_json)                      # {field: (raw value, confidence)}
    bill = {"bill_id": row.bill_id, "template": row.template, "pdf_path": row.pdf_path}
    confidences = {}
    for name, (kind, _code, _desc) in FIELDS.items():
        raw, conf = found[name]
        value = typed(kind, raw)
        bill[name], confidences[name] = value, conf
        field_rows.append({"bill_id": row.bill_id, "template": row.template, "field": name, "kind": kind,
                           "ai_raw": None if raw is None else str(raw),
                           "ai_value": None if value is None else str(value), "confidence": conf})

    # Billing days is not printed on the bill: calculate it from the two dates.
    start, end = bill["BillingStartDate"], bill["BillingEndDate"]
    bill["BillingDays"] = (dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days if start and end else None

    # --- Business rules: what a careful back-office clerk would check -------------------------
    reasons = [f"missing:{n}" for n in REQUIRED if bill[n] is None]
    reasons += [f"low_confidence:{n}" for n, c in confidences.items()
                if bill[n] is not None and c is not None and c < CONF_THRESHOLD]
    cups = norm_code(bill["CUPS"]) if bill["CUPS"] else None
    if cups and not CUPS_PATTERN.match(cups):
        reasons.append("cups_format")
    if bill["BillingDays"] is not None and not 1 <= bill["BillingDays"] <= 100:
        reasons.append("period_length")                         # a misread date makes the period negative or far too long
    if bill["ContractedPowerKW"] is not None and not 1 <= bill["ContractedPowerKW"] <= 15:
        reasons.append("power_out_of_range")                    # Spanish households: up to 15 kW
    if bill["TotalAmount"] is not None and bill["TotalAmount"] <= 0:
        reasons.append("total_not_positive")

    known = [c for c in confidences.values() if c is not None]
    bill.update({"min_confidence": min(known) if known else None,
                 "needs_review": bool(reasons), "review_reasons": "; ".join(reasons)})
    bills.append(bill)

save_table(field_rows, "bill_id:string, template:string, field:string, kind:string, ai_raw:string, "
           "ai_value:string, confidence:double", "silver_ai_fields")

# COMMAND ----------

# Silver step 2: the bills as a clean, typed business table (one row per bill).
LOWER_WORDS = {"de", "del", "la", "las", "los", "el", "y", "e", "da", "do", "das", "dos"}   # name particles stay lowercase

def clean_name(name):
    """'MARÍA DEL MAR TEJADA' or 'maría del mar tejada' -> 'María del Mar Tejada' (one style for every customer)."""
    if not name:
        return None
    words = []
    for i, word in enumerate(name.lower().split()):
        if i > 0 and word in LOWER_WORDS:
            words.append(word)
        else:                                                   # capital letter at the start and after - ' .
            words.append(re.sub(r"(^|[-'\u2019.])([^\W\d_])", lambda m: m.group(1) + m.group(2).upper(), word))
    return " ".join(words)

silver_bills = [{
    "bill_id": b["bill_id"], "template": b["template"], "pdf_path": b["pdf_path"],
    "invoice_number": b["InvoiceNumber"], "customer_name": clean_name(b["CustomerName"]),
    "marketer_name": b["MarketerName"], "cups": norm_code(b["CUPS"]) if b["CUPS"] else None,
    "contracted_power_kw": b["ContractedPowerKW"], "billing_start_date": b["BillingStartDate"],
    "billing_end_date": b["BillingEndDate"], "billing_days": b["BillingDays"],
    "energy_kwh": b["EnergyConsumedKWh"], "electricity_tax_eur": b["ElectricityTax"],
    "total_eur": b["TotalAmount"], "min_confidence": b["min_confidence"],
    "needs_review": b["needs_review"], "review_reasons": b["review_reasons"],
} for b in bills]
save_table(silver_bills, "bill_id:string, template:string, pdf_path:string, invoice_number:string, "
           "customer_name:string, marketer_name:string, cups:string, contracted_power_kw:double, "
           "billing_start_date:date, billing_end_date:date, billing_days:int, energy_kwh:double, "
           "electricity_tax_eur:double, total_eur:double, min_confidence:double, needs_review:bool, "
           "review_reasons:string", "silver_bills")
print(f"{sum(b['needs_review'] for b in silver_bills)} of {len(silver_bills)} bills need a human check")

# COMMAND ----------

# Silver step 3: the true values, typed the same way, so Gold can compare like with like.
truth_fields = []
for row in spark.read.table("bronze_ground_truth").collect():
    labels = flatten(json.loads(row.truth_json))
    for name, (kind, code, _desc) in FIELDS.items():
        value = typed(kind, truth_value(labels, code))
        truth_fields.append({"bill_id": row.bill_id, "field": name,
                             "true_value": None if value is None else str(value)})
save_table(truth_fields, "bill_id:string, field:string, true_value:string", "silver_truth_fields")

# COMMAND ----------

# Silver step 4: quarantine. A bill the AI could not read is kept here with the reason, so no bill disappears silently.
quarantine = []
bronze_status = {r.bill_id: r for r in spark.read.table("bronze_extractions").collect()}
for pdf in sorted(glob.glob(f"{LANDING}/*/*.pdf")):
    bid = bill_id_for(pdf)
    row = bronze_status.get(bid)
    if row is None:
        reason, detail = "not_processed", "no row in bronze_extractions: the AI step never ran for this bill"
    elif row.status != "Succeeded":
        reason, detail = "ai_failed", row.error
    else:
        continue                                                # read fine: it is in silver_bills (flagged if doubtful)
    quarantine.append({"bill_id": bid, "template": bid.split("__")[0], "pdf_path": pdf,
                       "reason": reason, "detail": detail})
save_table(quarantine, "bill_id:string, template:string, pdf_path:string, reason:string, detail:string",
           "silver_quarantine")
print(f"{len(quarantine)} bills quarantined. To retry them, run nb_01_bronze again, then this notebook again.")

# COMMAND ----------

# Look at the clean Silver bills: the first 10, now typed (real numbers and dates) and with the review flag. Only reads, changes nothing.
display(spark.sql("""
  SELECT bill_id, invoice_number, customer_name, marketer_name, cups, contracted_power_kw,
         billing_start_date, billing_end_date, billing_days, energy_kwh, electricity_tax_eur, total_eur,
         min_confidence, needs_review, review_reasons
  FROM silver_bills
  ORDER BY bill_id
  LIMIT 10
"""))
