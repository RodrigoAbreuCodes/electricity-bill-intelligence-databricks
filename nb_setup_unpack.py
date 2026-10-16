# Databricks notebook source
# Unpack the dataset zip from the volume into landing/T1 ... T6: the first PER_TEMPLATE bills of each template.
# Re-running is safe: files are overwritten.
import os, re, glob, shutil, zipfile

LANDING = "/Volumes/ebi-ai-project/bills/landing"              # path = /Volumes/<catalog>/<schema>/<volume>
ZIP_NAME = "training_example.zip"                               # the dataset zip uploaded to the volume
PER_TEMPLATE = 20                                               # 6 templates x 20 bills = 120
local_zip = "/tmp/bills.zip"
shutil.copy(f"{LANDING}/{ZIP_NAME}", local_zip)                # unzip from local disk, write whole files to the volume

def template_of(parts):
    """['training', 'template3', 'Invoice0001.pdf'] -> 'T3' (also accepts folders already called T3)."""
    for p in parts[:-1]:
        m = re.fullmatch(r"(?:template|T)\s*([1-6])", p, re.I)
        if m:
            return "T" + m.group(1)
    return None

found, skipped = {}, []                                         # found[template][bill name][".pdf" / ".json"] = zip member
with zipfile.ZipFile(local_zip) as z:
    for member in z.infolist():
        parts = [p for p in member.filename.replace("\\", "/").split("/") if p]
        if member.is_dir() or not parts or "__MACOSX" in parts:
            continue
        template = template_of(parts)
        stem, ext = os.path.splitext(parts[-1])
        if template is None or ext.lower() not in (".pdf", ".json") or "ann" in stem.lower():
            skipped.append(member.filename)                       # never let an annotated PDF in (Invoice0001_ann.pdf)
            continue
        found.setdefault(template, {}).setdefault(stem, {})[ext.lower()] = member

    written = 0
    for template, bills in sorted(found.items()):
        os.makedirs(f"{LANDING}/{template}", exist_ok=True)
        for stem in sorted(s for s, files in bills.items() if ".pdf" in files)[:PER_TEMPLATE]:
            for ext, member in bills[stem].items():
                with open(f"{LANDING}/{template}/{stem}{ext}", "wb") as out:
                    out.write(z.read(member))
                written += 1
print(f"{written} files written, {len(skipped)} skipped {skipped[:3]}")

# COMMAND ----------

# Check: 20 PDFs and 20 JSON files per template, and every PDF has its JSON.
for t in ["T1", "T2", "T3", "T4", "T5", "T6"]:
    pdfs = sorted(glob.glob(f"{LANDING}/{t}/*.pdf"))
    jsons = sorted(glob.glob(f"{LANDING}/{t}/*.json"))
    lonely = [os.path.basename(p) for p in pdfs if not os.path.exists(os.path.splitext(p)[0] + ".json")]
    print(f"{t}: {len(pdfs)} PDFs, {len(jsons)} JSON, PDFs without JSON: {lonely[:3]}")
