# Databricks notebook source
# MAGIC %run ./nb_utils

# COMMAND ----------

# One bill, all 10 fields: what the AI read, how sure it is, and the true value next to it.
pdf = sorted(glob.glob(f"{LANDING}/*/*.pdf"))[0]
print("Bill:", pdf)

row = spark.sql(f"""
  SELECT to_json(ai_extract(
           ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', '')),
           '{sql_text(EXTRACT_SCHEMA)}',
           map('version', '2.1', 'enableConfidenceScores', 'true', 'instructions', '{sql_text(INSTRUCTIONS)}')
         )) AS answer
  FROM read_files('{pdf}', format => 'binaryFile')
""").first()

found = ai_fields(row.answer)
js = os.path.splitext(pdf)[0] + ".json"
labels = flatten(json.loads(read_text(js))) if os.path.exists(js) else {}
print(f"   {'field':<18} {'AI read on the bill':<28} {'conf':>5}  true value")
for name, (kind, code, _desc) in FIELDS.items():
    raw, conf = found[name]
    ai, truth = typed(kind, raw), typed(kind, truth_value(labels, code))
    mark = {True: "OK ", False: "-- ", None: "   "}[is_correct(kind, ai, truth)]
    conf_txt = f"{conf:.2f}" if conf is not None else "  -"
    print(f"{mark}{name:<18} {str(raw)[:27]:<28} {conf_txt:>5}  {truth}")
