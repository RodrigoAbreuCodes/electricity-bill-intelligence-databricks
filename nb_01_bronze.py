# Databricks notebook source
# MAGIC %run ./nb_utils

# COMMAND ----------

# Bronze step 1: the table that keeps every raw AI answer. Failed bills are removed so this run retries them.
spark.sql("""
  CREATE TABLE IF NOT EXISTS bronze_extractions (
    bill_id STRING, template STRING, pdf_path STRING, status STRING, error STRING,
    extracted_json STRING, parsed_json STRING, extracted_at TIMESTAMP)
""")
failed = spark.sql("SELECT count(*) AS n FROM bronze_extractions WHERE status <> 'Succeeded'").first().n
spark.sql("DELETE FROM bronze_extractions WHERE status <> 'Succeeded'")
print(f"{failed} failed bills from earlier runs will be retried")

# COMMAND ----------

# Bronze step 2: the AI reads every new bill, one template folder at a time. Finished bills are skipped.
EXTRACT_SQL = """
INSERT INTO bronze_extractions
WITH files AS (
  SELECT regexp_replace(path, '^dbfs:', '') AS pdf_path, content
  FROM read_files('{folder}/*.pdf', format => 'binaryFile')
),
todo AS (
  SELECT * FROM files WHERE pdf_path NOT IN (SELECT pdf_path FROM bronze_extractions)
),
parsed AS (
  SELECT pdf_path, ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', '')) AS doc
  FROM todo
),
extracted AS (
  SELECT pdf_path, doc,
         ai_extract(doc, '{schema}',
                    map('version', '2.1', 'enableConfidenceScores', 'true', 'instructions', '{instructions}')) AS ext
  FROM parsed
)
SELECT
  concat(element_at(split(pdf_path, '/'), -2), '__',
         regexp_replace(element_at(split(pdf_path, '/'), -1), '[.]pdf$', '')) AS bill_id,
  element_at(split(pdf_path, '/'), -2) AS template,
  pdf_path,
  CASE WHEN ext IS NULL OR ext:error_message::string IS NOT NULL THEN 'Error' ELSE 'Succeeded' END AS status,
  CASE WHEN ext IS NULL THEN 'no answer from the AI' ELSE ext:error_message::string END AS error,
  to_json(ext) AS extracted_json,
  to_json(doc) AS parsed_json,
  current_timestamp() AS extracted_at
FROM extracted
"""

def bronze_count():
    return spark.sql("SELECT count(*) AS n FROM bronze_extractions").first().n

for folder in sorted(glob.glob(f"{LANDING}/T*")):
    before, started = bronze_count(), time.time()
    try:
        spark.sql(EXTRACT_SQL.format(folder=folder, schema=sql_text(EXTRACT_SCHEMA),
                                     instructions=sql_text(INSTRUCTIONS)))
    except Exception as e:                                      # one bad folder must not stop the others
        print(f"{os.path.basename(folder)}: FAILED, run the notebook again later. {str(e)[:300]}")
        continue
    added, secs = bronze_count() - before, time.time() - started
    per_bill = f", {secs / added:.1f}s per bill" if added else ""
    print(f"{os.path.basename(folder)}: {added} new bills read in {secs:.0f}s{per_bill}")

display(spark.sql("SELECT template, status, count(*) AS bills FROM bronze_extractions GROUP BY ALL ORDER BY ALL"))

# COMMAND ----------

# Bronze step 3: the true values, untouched, so Gold can score the AI later.
pdfs = sorted(glob.glob(f"{LANDING}/*/*.pdf"))
truth, missing = [], []
for pdf in pdfs:
    js = os.path.splitext(pdf)[0] + ".json"
    if os.path.exists(js):
        bid = bill_id_for(pdf)
        truth.append({"bill_id": bid, "template": bid.split("__")[0], "truth_json": read_text(js)})
    else:
        missing.append(pdf)
save_table(truth, "bill_id:string, template:string, truth_json:string", "bronze_ground_truth")
print(f"{len(missing)} PDFs without a matching .json {missing[:3]}")

# COMMAND ----------

# Look at what the AI wrote for the first 10 bills: one column per field, exactly as the AI copied it (nothing is cleaned yet).
# Only reads, changes nothing.
def ai_value(field):
    return f"get_json_object(extracted_json, '$.response.{field}.value') AS {field}"

display(spark.sql(f"""
  SELECT bill_id, status, {", ".join(ai_value(f) for f in FIELDS)}
  FROM bronze_extractions
  ORDER BY bill_id
  LIMIT 10
"""))
