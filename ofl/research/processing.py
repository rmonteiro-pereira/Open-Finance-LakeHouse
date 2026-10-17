"""Page-level text extraction/OCR, with immutable evidence and review candidates."""

import csv
import io
import json
import subprocess
from pathlib import Path

from .archive import add_candidate, import_csv_table, index_text
from .db import digest

VERSION = "pages-v1"


def _run(argv, *, timeout=180):
    return subprocess.run(argv, check=True, capture_output=True, timeout=timeout).stdout


def ocr_image(image: Path, psm=3):
    """TSV preserves word-level evidence. Confidence is not financial validation."""
    raw = _run(["tesseract", str(image), "stdout", "-l", "por+eng", "--psm", str(psm), "tsv"])
    words = []
    lines = {}
    for word in csv.DictReader(io.StringIO(raw.decode()), delimiter="\t"):
        if not word.get("text", "").strip():
            continue
        entry = {
            "text": word["text"],
            "confidence": float(word["conf"]),
            "bbox": [int(word[k]) for k in ("left", "top", "width", "height")],
            "segmentation": psm,
        }
        words.append(entry)
        group = tuple(word[k] for k in ("block_num", "par_num", "line_num"))
        lines.setdefault(group, []).append(word["text"])
    return "\n".join(" ".join(line) for line in lines.values()), words


def scan_tables(image: Path, words):
    """Detect ruled table regions; unruled pages remain manually selectable."""
    import cv2

    frame = cv2.imread(str(image), cv2.IMREAD_GRAYSCALE)
    if frame is None:
        raise ValueError("invalid page image")
    mask = cv2.adaptiveThreshold(~frame, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 15, -2)
    horizontal = cv2.morphologyEx(
        mask,
        cv2.MORPH_OPEN,
        cv2.getStructuringElement(cv2.MORPH_RECT, (max(20, frame.shape[1] // 30), 1)),
    )
    vertical = cv2.morphologyEx(
        mask,
        cv2.MORPH_OPEN,
        cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(20, frame.shape[0] // 30))),
    )
    contours, _ = cv2.findContours(
        horizontal | vertical, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        if w < frame.shape[1] * 0.2 or h < 50:
            continue
        grid = (horizontal | vertical)[y : y + h, x : x + w]
        xs = _runs([i for i, v in enumerate((grid > 0).sum(axis=0)) if v > h * 0.5])
        ys = _runs([i for i, v in enumerate((grid > 0).sum(axis=1)) if v > w * 0.5])
        if len(xs) < 2 or len(ys) < 2:
            continue
        cells, cell_boxes = [], []
        for ya, yb in zip(ys, ys[1:], strict=False):
            row, boxes = [], []
            for xa, xb in zip(xs, xs[1:], strict=False):
                cell_words = [
                    word
                    for word in words
                    if x + xa <= word["bbox"][0] + word["bbox"][2] / 2 < x + xb
                    and y + ya <= word["bbox"][1] + word["bbox"][3] / 2 < y + yb
                ]
                row.append(
                    " ".join(
                        v["text"]
                        for v in sorted(cell_words, key=lambda t: (t["bbox"][1], t["bbox"][0]))
                    )
                )
                boxes.append([x + xa, y + ya, x + xb, y + yb])
            cells.append(row)
            cell_boxes.append(boxes)
        yield [x, y, x + w, y + h], cells, {"cell_boxes": cell_boxes, "coordinate_space": "pixels"}


def _runs(values):
    groups = []
    for value in values:
        if not groups or value > groups[-1][-1] + 1:
            groups.append([])
        groups[-1].append(value)
    return [sum(group) // len(group) for group in groups]


def process_asset(db, store, asset_id, heartbeat=lambda _: None):
    with db.connect() as guard:
        guard.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (asset_id,))
        asset = guard.execute("SELECT * FROM assets WHERE id=%s", (asset_id,)).fetchone()
        if not asset or not asset["active"] or asset["state"] == "processed":
            return
        path = store.path(asset["object_key"])
        media = asset["media_type"]
        if media.startswith(("audio/", "video/")) or media == "application/octet-stream":
            guard.execute("UPDATE assets SET state='metadata_only' WHERE id=%s", (asset_id,))
            return
        if media == "text/csv":
            import_csv_table(guard, asset, path)
        elif media.startswith("text/"):
            index_text(
                guard,
                item_id=asset["item_id"],
                asset_id=asset_id,
                revision=digest([asset_id, VERSION]),
                text=path.read_text(encoding="utf-8"),
                method="text",
            )
        elif media == "application/pdf":
            import pdfplumber

            # Each page is committed independently so a late OCR failure is resumable.
            with pdfplumber.open(path) as pdf:
                for number, page in enumerate(pdf.pages, 1):
                    _page(db, store, asset, number, path, page, heartbeat)
                    page.close()
        elif media.startswith("image/"):
            _page(db, store, asset, 1, path, None, heartbeat)
        else:
            guard.execute("UPDATE assets SET state='metadata_only' WHERE id=%s", (asset_id,))
            return
        guard.execute("UPDATE assets SET state='processed',error=NULL WHERE id=%s", (asset_id,))


def _page(db, store, asset, number, path, page, heartbeat):
    revision = digest([asset["sha256"], VERSION, number])
    key = f"pages/{asset['id']}/{number}"
    folder = store.path(key)
    folder.mkdir(parents=True, exist_ok=True)
    marker = folder / "complete.json"
    if marker.exists() and json.loads(marker.read_text()).get("revision") == revision:
        heartbeat({"asset_id": asset["id"], "page": number, "resumed": True})
        return
    store.check_space(64 * 1024**2)
    text = page.extract_text() or "" if page else ""
    words = []
    image = folder / "preview.png"
    if page:
        _run(
            [
                "pdftoppm",
                "-f",
                str(number),
                "-l",
                str(number),
                "-scale-to",
                "2200",
                "-singlefile",
                "-png",
                str(path),
                str(folder / "preview"),
            ]
        )
    else:
        from PIL import Image, ImageOps

        with Image.open(path) as img:
            img = ImageOps.exif_transpose(img)
            img.thumbnail((2200, 2200))
            img.convert("RGB").save(image)
    method = "text"
    candidates = []
    if len(text.strip()) < 40:
        text, words = ocr_image(image)
        method = "ocr"
        candidates = list(scan_tables(image, words))
        if candidates:
            # Automatic page segmentation can treat ruled cells as graphics.
            # A separate uniform-block pass supplies their words/coordinates.
            _, table_words = ocr_image(image, psm=6)
            candidates = list(scan_tables(image, table_words))
            words.extend(table_words)
            for _, cells, _ in candidates:
                table_text = "\n".join(" | ".join(row) for row in cells)
                if any(value and value not in text for row in cells for value in row):
                    text += "\n" + table_text
    elif page:
        for table in page.find_tables():
            candidates.append(
                (
                    list(table.bbox),
                    table.extract(),
                    {
                        "cell_boxes": [
                            [list(cell) if cell else None for cell in row.cells]
                            for row in table.rows
                        ],
                        "coordinate_space": "pdf_points",
                        "page_width": page.width,
                        "page_height": page.height,
                    },
                )
            )
    evidence = {
        "preview": key + "/preview.png",
        "words": words,
        "coordinate_space": "pixels" if method == "ocr" else "pdf_points",
    }
    (folder / "text.txt").write_text(text, encoding="utf-8")
    (folder / "evidence.json").write_text(json.dumps(evidence, ensure_ascii=False))
    with db.connect() as con:
        # A revision that switches text/OCR methods supersedes the old page too.
        con.execute(
            "UPDATE passages SET active=false WHERE asset_id=%s AND page=%s", (asset["id"], number)
        )
        index_text(
            con,
            item_id=asset["item_id"],
            asset_id=asset["id"],
            page=number,
            revision=revision,
            text=text,
            method=method,
            evidence={k: v for k, v in evidence.items() if k != "words"},
        )
        for bbox, cells, table_evidence in candidates:
            add_candidate(con, asset["id"], number, bbox, cells, VERSION, table_evidence)
    # Marker follows the database commit; replay before it is idempotent.
    temp = marker.with_suffix(".tmp")
    temp.write_text(json.dumps({"revision": revision}))
    temp.replace(marker)
    heartbeat({"asset_id": asset["id"], "page": number})
