import json, re, sys
from pathlib import Path
from hashlib import sha1

# Try OCR imports (optional). If not present, we’ll skip OCR gracefully.
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_OK = True
except Exception:
    OCR_OK = False

# Required extractor
try:
    from pdfminer.high_level import extract_text
except Exception:
    print("pdfminer.six is required. Install with: poetry add pdfminer.six")
    sys.exit(1)

RAW  = Path("data/raw")
PROC = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)
LIST = Path("data/audits/pdf_urls_discovered.txt")

if not LIST.exists():
    print("Missing data/audits/pdf_urls_discovered.txt"); sys.exit(1)

def wc(s:str)->int:
    return len(re.findall(r"\w+", s or ""))

def sid_for(url:str)->str:
    return sha1(url.encode("utf-8")).hexdigest()[:16]

def find_pdf(sid:str):
    m = list(RAW.glob(f"{sid}_*.pdf"))
    return m[0] if m else None

def ocr_pdf(path:Path)->str:
    pages = convert_from_path(str(path))
    parts = []
    for img in pages:
        parts.append(pytesseract.image_to_string(img, lang="deu+fra+ita+eng"))
    return "\n".join(parts)

urls = [u.strip() for u in LIST.read_text(encoding="utf-8").splitlines() if u.strip()]
created=skipped=missing=failed=ocr_used=need_ocr=0

for u in urls:
    sid = sid_for(u)
    out_json = PROC / f"{sid}.json"
    if out_json.exists():
        skipped += 1
        continue

    pdf_path = find_pdf(sid)
    if not pdf_path:
        missing += 1
        continue

    text = ""
    try:
        text = extract_text(str(pdf_path)) or ""
    except Exception:
        text = ""

    if wc(text) < 20:
        if OCR_OK:
            try:
                text = ocr_pdf(pdf_path) or text
                ocr_used += 1
            except Exception:
                need_ocr += 1
        else:
            need_ocr += 1

    data = {
        "url": u,
        "source_type": "pdf",
        "content_type": "application/pdf",
        "text": text,
        "word_count": wc(text),
        "ocr_applied": (wc(text) < 20 and OCR_OK)
    }
    try:
        out_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        created += 1
    except Exception:
        failed += 1

print("=== Parse New PDFs from list ===")
print(f"URLs in list:              {len(urls)}")
print(f"New processed JSONs:       {created}")
print(f"Already existed (skipped): {skipped}")
print(f"Missing raw PDF files:     {missing}")
print(f"OCR used:                  {ocr_used}")
print(f"Needed OCR but unavailable:{need_ocr}")
print(f"Failures to write:         {failed}")