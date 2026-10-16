# Databricks notebook source
# nb_utils: settings and helpers shared by every notebook (each one starts with %run ./nb_utils).
import os, re, json, glob, time, unicodedata, datetime as dt
from pyspark.sql import types as T

# --- Settings: the only lines you edit ---------------------------------
CATALOG = "ebi-ai-project"                  # the catalog that holds the project (create it first; plain name here, SQL gets backticks below)
SCHEMA = "bills"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"   # T1 ... T6, each with PDFs + truth JSON
CONF_THRESHOLD = 0.80                       # below this, a human checks the field

spark.sql(f"USE CATALOG `{CATALOG}`")      # backticks: the hyphen in the name needs them in SQL
spark.sql(f"USE SCHEMA {SCHEMA}")

# The 10 fields the AI reads: name -> (how to compare it, IDSEM ground-truth code, instruction for the AI)
FIELDS = {
    "InvoiceNumber":     ("code",    "F1", "Invoice number (numero de factura), exactly as printed"),
    "CustomerName":      ("text",    "A1", "Full name of the customer (titular) the bill is addressed to"),
    "MarketerName":      ("text",    "C1", "Name of the electricity company (comercializadora) that issues the bill"),
    "CUPS":              ("code",    "E1", "CUPS supply point code: ES followed by 16 digits and 2 letters, sometimes 2 more characters"),
    "ContractedPowerKW": ("number",  "E3", "Contracted power (potencia contratada) in kW; if there are several periods, the first one (P1 / punta)"),
    "BillingStartDate":  ("date",    "F4", "First day of the billing period (periodo de facturacion)"),
    "BillingEndDate":    ("date",    "F5", "Last day of the billing period"),
    # BillingDays (F6) is not printed on the bill: Silver calculates it from the two dates.
    "EnergyConsumedKWh": ("number",  "I3", "Total electricity consumed in the billing period, in kWh"),
    "ElectricityTax":    ("number",  "N2", "Electricity tax amount (impuesto sobre la electricidad / impuesto electrico) in euros"),
    "TotalAmount":       ("number",  "J5", "Total amount of the invoice (total importe factura) in euros, taxes included"),
}
INSTRUCTIONS = ("These are Spanish household electricity bills. Copy every value exactly as printed on the bill, "
                "including its original number and date format. Leave a field empty if it is not on the bill.")

# Every field is asked as a string: the AI copies what it sees, and Silver types it (one place for Spanish formats).
EXTRACT_SCHEMA = json.dumps({name: {"type": "string", "description": desc}
                             for name, (_kind, _code, desc) in FIELDS.items()}, ensure_ascii=False)

# COMMAND ----------

# Writing tables and reading files
TYPES = {"string": T.StringType(), "double": T.DoubleType(), "int": T.IntegerType(),
         "date": T.DateType(), "bool": T.BooleanType()}

def make_schema(spec):
    """'bill_id:string, total:double' -> Spark schema. Explicit types: empty columns never break a write."""
    return T.StructType([T.StructField(n.strip(), TYPES[t.strip()], True)
                         for n, t in (c.split(":") for c in spec.split(","))])

def save_table(records, spec, name):
    """Write a list of dicts as a Delta table (overwrite) with the column types in spec."""
    schema = make_schema(spec)
    def conv(v, f):
        if v is None or (isinstance(v, float) and v != v):     # None or NaN -> null
            return None
        if isinstance(f.dataType, T.DateType) and isinstance(v, str):
            return dt.date.fromisoformat(v)
        if isinstance(f.dataType, T.DoubleType):
            return float(v)
        if isinstance(f.dataType, T.IntegerType):
            return int(v)
        return v
    rows = [tuple(conv(r.get(f.name), f) for f in schema.fields) for r in records]
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(name)
    print(f"{name}: {len(rows)} rows")
    return df

def read_text(path):
    for enc in ("utf-8-sig", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue

def bill_id_for(path):
    """.../landing/T3/factura_0042.pdf -> 'T3__factura_0042' (template folder + file name)."""
    template = os.path.basename(os.path.dirname(path))
    stem = os.path.splitext(os.path.basename(path))[0]
    return f"{template}__{stem}"

def sql_text(s):
    """Make a Python string safe inside a single-quoted SQL literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")

# COMMAND ----------

# Reading Spanish numbers, dates and names
MONTHS = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7,
          "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
MONTHS3 = {k[:3]: v for k, v in MONTHS.items()}                # ene, feb, mar ... set, oct, nov, dic

def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def parse_number(value):
    """'1.234,56 EUR' -> 1234.56 | '3,45 kW' -> 3.45 | '12,14 EUR,' -> 12.14 | '3,13 kW x 60 dias x 0,115 EUR/kW' -> 3.13
    (Spanish: dot = thousands, comma = decimals; only the FIRST number on the line counts)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = re.sub(r"(?<=\d)\s(?=\d{3}(?!\d))", "", str(value))   # '1 234,56' -> '1234,56'
    m = re.search(r"-?\d[\d.,]*", text)                           # the first number in the text
    if not m:
        return None
    s = m.group(0).rstrip(".,")                                    # '12,14,' -> '12,14'
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
    elif "," in s:
        s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 else s.replace(",", "")
    elif s.count(".") > 1 or (s.count(".") == 1 and len(s.split(".")[1]) == 3):
        s = s.replace(".", "")                                      # 1.234 -> 1234
    try:
        return float(s)
    except ValueError:
        return None

def parse_date(value):
    """'2021-03-01', '01/03/2021', '01.03.21', '1 de marzo de 2021', '01-mar-2021' -> '2021-03-01'."""
    if not value:
        return None
    s = strip_accents(str(value)).lower()
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = map(int, m.groups())
    else:
        m = re.search(r"(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{2,4})", s)
        if m:
            d, mo, y = map(int, m.groups())
        else:
            m = re.search(r"(\d{1,2})[\s\-/.]*(?:de\s+)?([a-z]+)\.?[\s\-/.]*(?:de\s+|del\s+)?(\d{2,4})", s)
            if not m:
                return None
            d, y = int(m.group(1)), int(m.group(3))
            mo = MONTHS.get(m.group(2)) or MONTHS3.get(m.group(2)[:3])
            if not mo:
                return None
        y = y + 2000 if y < 100 else y
    try:
        return dt.date(y, mo, d).isoformat()
    except ValueError:
        return None

def norm_text(s):
    """'Dna. Maria  Perez-Gil' -> 'DNA MARIA PEREZ GIL' (case, accents, punctuation and spaces ignored)."""
    return " ".join(re.sub(r"[^A-Z0-9]+", " ", strip_accents(str(s)).upper()).split()) if s is not None else None

def norm_code(s):
    """'ES 0021 0000 1234 5678 AB' -> 'ES0021000012345678AB' (spaces and symbols removed)."""
    return re.sub(r"[^A-Z0-9]", "", strip_accents(str(s)).upper()) if s is not None else None

def typed(kind, value):
    """Turn a raw value (AI or ground truth) into the comparable form for its kind."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if kind == "number":
        return parse_number(value)
    if kind == "integer":
        n = parse_number(value)
        return int(round(n)) if n is not None else None
    if kind == "date":
        return parse_date(value)
    return str(value).strip()

# COMMAND ----------

# Reading the true values (any JSON layout -> {IDSEM code: value}) and comparing them with the AI
CODE_KEY = re.compile(r"[A-Z][0-9A-Z][a-z]*")                 # A1, E1, F4l, CD ...

def flatten(obj, out=None):
    """Any JSON layout -> {IDSEM code: value}: flat files, nested groups, or codes wrapped in objects."""
    out = {} if out is None else out
    items = obj.items() if isinstance(obj, dict) else enumerate(obj) if isinstance(obj, list) else []
    for k, v in items:
        if not isinstance(v, (dict, list)):
            out.setdefault(str(k), v)
            continue
        inner = flatten(v)
        if CODE_KEY.fullmatch(str(k)) and not any(CODE_KEY.fullmatch(x) for x in inner):
            out.setdefault(str(k), next((x for x in inner.values() if str(x).strip()), None))
        else:
            for ik, iv in inner.items():
                out.setdefault(ik, iv)
    return out

def truth_value(labels, code):
    """Value for an IDSEM code; also accepts format variants such as F4l / F4s / F4p."""
    for key in sorted(labels, key=len):
        if re.fullmatch(rf"{code}[a-z]*", key) and labels[key] is not None and str(labels[key]).strip():
            return labels[key]
    return None

def ai_fields(extracted_json):
    """ai_extract answer -> {field: (raw value, confidence)}; accepts the 2.1 shape and the plain 2.0 shape."""
    response = (json.loads(extracted_json) or {}).get("response") or {}
    out = {}
    for name in FIELDS:
        f = response.get(name)
        if isinstance(f, dict):
            out[name] = (f.get("value"), f.get("confidence_score"))
        else:
            out[name] = (f, None)
    return out

def is_correct(kind, ai, truth):
    if truth is None:
        return None                                             # no truth for this field: not scored
    if ai is None:
        return False
    if kind == "number":
        a, t = float(ai), float(truth)
        return abs(a - t) <= max(0.011, 0.001 * abs(t))        # 1 cent, or 0.1% for big numbers
    if kind == "integer":
        return int(float(ai)) == int(float(truth))
    if kind == "code":
        return norm_code(ai) == norm_code(truth)
    if kind == "text":
        return norm_text(ai) == norm_text(truth)
    return ai == truth                                          # dates: both ISO yyyy-mm-dd
