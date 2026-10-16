# Databricks notebook source
# MAGIC %run ./nb_utils

# COMMAND ----------

# Gold step 1: score the AI. One row per bill and field: what the AI said, the truth, right or wrong.
from collections import Counter, defaultdict
from difflib import SequenceMatcher

truth = {(r.bill_id, r.field): r.true_value for r in spark.read.table("silver_truth_fields").collect()}
review = {r.bill_id: r.needs_review for r in spark.read.table("silver_bills").select("bill_id", "needs_review").collect()}

accuracy = []
for r in spark.read.table("silver_ai_fields").collect():
    t = truth.get((r.bill_id, r.field))
    ok = is_correct(r.kind, r.ai_value, t)
    accuracy.append({
        "bill_id": r.bill_id, "template": r.template, "field": r.field, "ai_value": r.ai_value,
        "true_value": t, "confidence": r.confidence,
        "is_scored": ok is not None, "is_correct": None if ok is None else int(ok),
        "similarity": None if (r.ai_value is None or t is None) else round(SequenceMatcher(None, str(r.ai_value), str(t)).ratio(), 3),
        "low_confidence": r.confidence is not None and r.confidence < CONF_THRESHOLD,
        "bill_in_review": bool(review.get(r.bill_id)),
    })
save_table(accuracy, "bill_id:string, template:string, field:string, ai_value:string, true_value:string, "
           "confidence:double, is_scored:bool, is_correct:int, similarity:double, low_confidence:bool, "
           "bill_in_review:bool", "fact_field_accuracy")

# COMMAND ----------

# Gold step 2: the bill fact table, the review queue and the AI scorecard.
wrong = Counter(a["bill_id"] for a in accuracy if a["is_correct"] == 0)
fact_bill = []
for r in spark.read.table("silver_bills").collect():
    b = r.asDict()
    b["n_fields_wrong"] = wrong.get(b["bill_id"], 0)
    b["has_ai_error"] = b["n_fields_wrong"] > 0
    b["eur_per_kwh"] = round(b["total_eur"] / b["energy_kwh"], 4) if b["total_eur"] and b["energy_kwh"] else None
    fact_bill.append(b)
BILL_SPEC = ("bill_id:string, template:string, pdf_path:string, invoice_number:string, customer_name:string, "
             "marketer_name:string, cups:string, contracted_power_kw:double, billing_start_date:date, "
             "billing_end_date:date, billing_days:int, energy_kwh:double, electricity_tax_eur:double, "
             "total_eur:double, eur_per_kwh:double, min_confidence:double, needs_review:bool, "
             "review_reasons:string, has_ai_error:bool, n_fields_wrong:int")
save_table(fact_bill, BILL_SPEC, "fact_bill")

queue = sorted((b for b in fact_bill if b["needs_review"]), key=lambda b: b["min_confidence"] or 0)
save_table(queue, "bill_id:string, template:string, pdf_path:string, review_reasons:string, "
           "min_confidence:double, total_eur:double, customer_name:string", "gold_review_queue")

by_field = defaultdict(list)
for a in accuracy:
    if a["is_scored"]:
        by_field[a["field"]].append(a)
def pct(rows):
    return round(100 * sum(x["is_correct"] for x in rows) / len(rows), 1) if rows else None
scorecard = []
for field, rows in sorted(by_field.items()):
    sure = [x for x in rows if not x["low_confidence"]]
    unsure = [x for x in rows if x["low_confidence"]]
    confs = [x["confidence"] for x in rows if x["confidence"] is not None]
    scorecard.append({"field": field, "bills_scored": len(rows), "accuracy_pct": pct(rows),
                      "avg_confidence": round(sum(confs) / len(confs), 3) if confs else None,
                      "accuracy_when_confident_pct": pct(sure), "accuracy_when_unsure_pct": pct(unsure),
                      "bills_unsure": len(unsure)})
save_table(scorecard, "field:string, bills_scored:int, accuracy_pct:double, avg_confidence:double, "
           "accuracy_when_confident_pct:double, accuracy_when_unsure_pct:double, bills_unsure:int",
           "gold_ai_scorecard")

# COMMAND ----------

# Gold step 3: small dimensions for the dashboard (template and calendar).
templates = sorted({b["template"] for b in fact_bill})
save_table([{"template": t,
             "bills": sum(1 for b in fact_bill if b["template"] == t),
             "marketers": len({b["marketer_name"] for b in fact_bill if b["template"] == t and b["marketer_name"]})}
            for t in templates],
           "template:string, bills:int, marketers:int", "dim_template")

days = [d for b in fact_bill for d in (b["billing_start_date"], b["billing_end_date"]) if d]
calendar, d = [], min(days, default=dt.date.today())
while d <= max(days, default=d):
    calendar.append({"date": d, "year": d.year, "month": d.month, "month_name": d.strftime("%b"),
                     "year_month": d.strftime("%Y-%m")})
    d += dt.timedelta(days=1)
save_table(calendar, "date:date, year:int, month:int, month_name:string, year_month:string", "dim_date")

# COMMAND ----------

# The headline numbers.
scored = [a for a in accuracy if a["is_scored"]]
with_error = [b for b in fact_bill if b["has_ai_error"]]
caught = [b for b in with_error if b["needs_review"]]
quarantined = spark.read.table("silver_quarantine").count()
n_pdfs = len(glob.glob(f"{LANDING}/*/*.pdf"))
print(f"Bills processed:          {len(fact_bill)}")
print(f"Bills quarantined:        {quarantined}   (check: {len(fact_bill)} + {quarantined} = {len(fact_bill) + quarantined} of {n_pdfs} PDFs)")
print(f"Field accuracy:           {pct(scored)}% of {len(scored)} fields")
print(f"Bills sent to review:     {round(100 * len(queue) / len(fact_bill), 1)}%")
print(f"Bills with an AI error:   {len(with_error)}, of which caught by review: {len(caught)}"
      f" ({round(100 * len(caught) / len(with_error), 1) if with_error else 100.0}%)")
display(spark.read.table("gold_ai_scorecard").orderBy("accuracy_pct"))

# COMMAND ----------

# Look at the first 10 rows of every Gold table. Only reads, changes nothing.
GOLD_TABLES = {"fact_field_accuracy": "bill_id, field", "fact_bill": "bill_id", "gold_review_queue": "min_confidence",
               "gold_ai_scorecard": "accuracy_pct", "dim_template": "template", "dim_date": "date"}
for table, order_by in GOLD_TABLES.items():
    print(f"--- {table}: {spark.read.table(table).count()} rows (showing the first 10) ---")
    display(spark.sql(f"SELECT * FROM {table} ORDER BY {order_by} LIMIT 10"))
