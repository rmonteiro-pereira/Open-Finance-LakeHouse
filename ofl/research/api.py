"""Private research API. Gateway identity + shared secret; no public routes."""

import hashlib
import hmac
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from .config import ResearchSettings
from .db import Database, digest, uid
from .imports import coverage, register
from .storage import Store


def response(value, status=200):
    return JSONResponse(jsonable_encoder(value, custom_encoder={Decimal: str}), status_code=status)


class UploadStart(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    source: Literal["b3_cotahist", "b3_bulletin", "mub3"]
    size: int = Field(ge=1)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    metadata: dict = Field(default_factory=dict)


class ReviewBody(BaseModel):
    expected_revision: int = Field(ge=0)
    state: Literal["approved", "rejected", "needs_review"]
    cells: list[list[str | None]]
    mapping: dict = Field(default_factory=dict)


class JobAction(BaseModel):
    action: Literal["embed", "build_observations", "pipeline"]
    dag: str | None = None


class ExportBody(BaseModel):
    filters: dict = Field(default_factory=dict)


class ReprocessBody(BaseModel):
    supersedes: str | None = None


class ManualCandidate(BaseModel):
    asset_id: str
    page: int = Field(ge=1)
    bbox: list[float] = Field(min_length=4, max_length=4)
    cells: list[list[str | None]]


def create_app(settings=None):
    settings = settings or ResearchSettings()
    db = Database(settings)
    store = Store.from_settings(settings)
    app = FastAPI(title="OFL private research", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.db, app.state.store = db, store

    def identity(request: Request):
        token = request.headers.get("x-ofl-gateway-token", "")
        if not settings.gateway_token or not hmac.compare_digest(token, settings.gateway_token):
            raise HTTPException(401, "verified private gateway required")
        actor = request.headers.get("x-ofl-identity", "")
        try:
            roles = json.loads(settings.roles)
        except (ValueError, TypeError):
            raise HTTPException(503, "roles are not configured") from None
        role = roles.get(actor) if isinstance(roles, dict) else None
        if role not in {"reader", "reviewer", "admin"}:
            raise HTTPException(403, "identity has no research access")
        if request.method not in {"GET", "HEAD"}:
            if request.headers.get("origin") != settings.origin:
                raise HTTPException(403, "same-origin request required")
        return actor, role

    def admin(user=Depends(identity)):
        if user[1] != "admin":
            raise HTTPException(403, "administrator role required")
        return user

    def reviewer(user=Depends(identity)):
        if user[1] not in {"reviewer", "admin"}:
            raise HTTPException(403, "reviewer role required")
        return user

    @app.middleware("http")
    async def bounds(request, call_next):
        if request.method not in {"GET", "HEAD"} and not (
            request.method == "PUT" and "/uploads/" in request.url.path
        ):
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 4 * 1024**2:
                    return response({"detail": "request body too large"}, 413)
            request._body = bytes(body)
        result = await call_next(request)
        result.headers["Cache-Control"] = "no-store"
        result.headers["X-Content-Type-Options"] = "nosniff"
        return result

    @app.exception_handler(ValueError)
    async def invalid(_request, exc):
        return response({"detail": str(exc)}, 400)

    @app.exception_handler(LookupError)
    async def missing(_request, _exc):
        return response({"detail": "record not found"}, 404)

    @app.exception_handler(FileExistsError)
    async def conflict(_request, exc):
        return response({"detail": str(exc)}, 409)

    def idempotent(request, actor, payload, operation):
        key = request.headers.get("idempotency-key", "")
        if not key or len(key) > 128:
            raise HTTPException(400, "Idempotency-Key required (maximum 128 characters)")
        fingerprint = digest([request.method, request.url.path, payload])
        with db.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (actor + key,))
            previous = con.execute(
                "SELECT * FROM requests WHERE actor=%s AND key=%s", (actor, key)
            ).fetchone()
            if previous:
                if previous["digest"] != fingerprint:
                    raise HTTPException(409, "idempotency key reused for different request")
                return previous["response"]
            result = operation(con)
            con.execute(
                "INSERT INTO requests(actor,key,digest,response) VALUES (%s,%s,%s,%s)",
                (actor, key, fingerprint, Jsonb(result)),
            )
            return result

    @app.get("/api/v1/coverage")
    def get_coverage(_user=Depends(identity)):
        return response(coverage(db))

    @app.get("/api/v1/me")
    def me(user=Depends(identity)):
        return response(
            {
                "identity": user[0],
                "role": user[1],
                "semantic_enabled": bool(settings.embedding_revision),
            }
        )

    @app.get("/api/v1/search")
    def get_search(
        q: str = Query(min_length=1, max_length=1000),
        mode: Literal["keyword", "vector", "hybrid"] = "keyword",
        source: str | None = None,
        item_type: str | None = None,
        collection: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = Query(20, ge=1, le=100),
        _user=Depends(identity),
    ):
        from .search import search

        if mode != "keyword" and not settings.embedding_revision:
            raise HTTPException(
                503, "Semantic search is not configured; keyword search is available"
            )

        return response(
            search(
                db,
                q,
                mode=mode,
                source=source,
                item_type=item_type,
                collection=collection,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
            )
        )

    @app.get("/api/v1/items")
    def items(
        source: str | None = None,
        item_type: str | None = None,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        _user=Depends(identity),
    ):
        return response(
            db.rows(
                """SELECT * FROM items WHERE (%s::text IS NULL OR source=%s)
            AND (%s::text IS NULL OR item_type=%s) ORDER BY title,id LIMIT %s OFFSET %s""",
                (source, source, item_type, item_type, limit, offset),
            )
        )

    @app.get("/api/v1/items/{item_id}")
    def item(item_id: str, _user=Depends(identity)):
        rows = db.rows("SELECT * FROM items WHERE id=%s", (item_id,))
        if not rows:
            raise LookupError()
        return response(
            {
                "item": rows[0],
                "assets": db.rows(
                    "SELECT id,name,media_type,state,error FROM assets WHERE item_id=%s AND active",
                    (item_id,),
                ),
                "pages": db.rows(
                    "SELECT id,asset_id,page,text,method,evidence FROM passages p WHERE item_id=%s AND active AND (asset_id IS NULL OR EXISTS(SELECT 1 FROM assets a WHERE a.id=p.asset_id AND a.active)) ORDER BY asset_id,page,ordinal",
                    (item_id,),
                ),
            }
        )

    @app.get("/api/v1/assets/{asset_id}")
    def asset(asset_id: str, _user=Depends(identity)):
        rows = db.rows("SELECT * FROM assets WHERE id=%s", (asset_id,))
        if not rows:
            raise LookupError()
        row = rows[0]
        return FileResponse(
            store.path(row["object_key"]), media_type=row["media_type"], filename=row["name"]
        )

    @app.get("/api/v1/assets/{asset_id}/pages/{page}")
    def page(asset_id: str, page: int, _user=Depends(identity)):
        if page < 1 or not db.rows("SELECT id FROM assets WHERE id=%s", (asset_id,)):
            raise LookupError()
        path = store.path(f"pages/{asset_id}/{page}/preview.png")
        if not path.exists():
            raise LookupError()
        return FileResponse(path, media_type="image/png")

    @app.get("/api/v1/quotes")
    def quotes(
        symbol: str | None = None,
        market: str | None = None,
        currency: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        release: str | None = None,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        _user=Depends(identity),
    ):
        from .query import quote_query

        return response(
            quote_query(
                db,
                symbol=symbol,
                market=market,
                currency=currency,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                offset=offset,
                release=release,
            )
        )

    @app.get("/api/v1/observations")
    def observations(
        subject: str | None = None, limit: int = Query(100, ge=1, le=1000), _user=Depends(identity)
    ):
        return response(
            db.rows(
                """SELECT * FROM observations WHERE active
            AND (%s::text IS NULL OR subject=%s) ORDER BY date,id LIMIT %s""",
                (subject, subject, limit),
            )
        )

    @app.get("/api/v1/imports")
    def imports(limit: int = Query(100, ge=1, le=1000), _user=Depends(identity)):
        return response(
            db.rows(
                "SELECT id,source,name,size,state,counts,error,created_at FROM imports ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
        )

    @app.get("/api/v1/jobs")
    def jobs(limit: int = Query(100, ge=1, le=1000), _user=Depends(identity)):
        return response(db.rows("SELECT * FROM jobs ORDER BY created_at DESC LIMIT %s", (limit,)))

    @app.post("/api/v1/imports/{import_id}/reprocess")
    def reprocess_import(import_id: str, body: ReprocessBody, user=Depends(admin)):
        from .imports import reprocess

        return response(reprocess(db, import_id, actor=user[0], supersedes=body.supersedes), 202)

    @app.post("/api/v1/jobs")
    def action(body: JobAction, request: Request, user=Depends(admin)):
        from .orchestration import ALLOWED_DAGS

        if body.action == "pipeline" and body.dag not in ALLOWED_DAGS:
            raise ValueError("DAG not allowlisted")
        return response(
            idempotent(
                request,
                user[0],
                body.model_dump(),
                lambda con: {
                    "job_id": db.enqueue_on(
                        con, body.action, {"dag": body.dag} if body.dag else {}, user[0]
                    )
                },
            ),
            202,
        )

    @app.post("/api/v1/jobs/{job_id}/retry")
    def retry(job_id: str, request: Request, user=Depends(admin)):
        def operation(con):
            row = con.execute("SELECT * FROM jobs WHERE id=%s FOR UPDATE", (job_id,)).fetchone()
            if not row:
                raise LookupError()
            if row["state"] != "failed":
                raise FileExistsError("only failed jobs can be retried")
            con.execute("UPDATE jobs SET state='queued',error=NULL WHERE id=%s", (job_id,))
            con.execute(
                "INSERT INTO audit(actor,action,target) VALUES (%s,'retry',%s)", (user[0], job_id)
            )
            return {"job_id": job_id}

        return response(idempotent(request, user[0], {}, operation), 202)

    @app.get("/api/v1/candidates")
    def candidates(state: str = "needs_review", _user=Depends(reviewer)):
        return response(
            db.rows("SELECT * FROM candidates WHERE state=%s ORDER BY id LIMIT 100", (state,))
        )

    @app.post("/api/v1/candidates")
    def manual_candidate(body: ManualCandidate, request: Request, user=Depends(reviewer)):
        from .archive import add_candidate

        def operation(con):
            if not con.execute("SELECT id FROM assets WHERE id=%s", (body.asset_id,)).fetchone():
                raise LookupError()
            if len(body.cells) > 5000:
                raise ValueError("table too large")
            from PIL import Image

            preview = store.path(f"pages/{body.asset_id}/{body.page}/preview.png")
            if not preview.exists():
                raise ValueError("page preview is required to locate a manual region")
            with Image.open(preview) as image:
                width, height = image.size
            bbox = body.bbox
            # [0,0,1,1] selects the complete page, not a fabricated one-pixel cell.
            if bbox == [0, 0, 1, 1]:
                bbox = [0, 0, width, height]
            if not (0 <= bbox[0] < bbox[2] <= width and 0 <= bbox[1] < bbox[3] <= height):
                raise ValueError("region lies outside the source page")
            return {
                "candidate_id": add_candidate(
                    con,
                    body.asset_id,
                    body.page,
                    bbox,
                    body.cells,
                    "manual-v1",
                    {"coordinate_space": "pixels", "page_width": width, "page_height": height},
                )
            }

        return response(idempotent(request, user[0], body.model_dump(), operation), 201)

    @app.get("/api/v1/candidates/{candidate_id}")
    def candidate(candidate_id: str, _user=Depends(reviewer)):
        rows = db.rows("SELECT * FROM candidates WHERE id=%s", (candidate_id,))
        if not rows:
            raise LookupError()
        return response(
            {
                "candidate": rows[0],
                "reviews": db.rows(
                    "SELECT * FROM reviews WHERE candidate_id=%s ORDER BY revision", (candidate_id,)
                ),
            }
        )

    @app.post("/api/v1/candidates/{candidate_id}/review")
    def review(candidate_id: str, body: ReviewBody, user=Depends(reviewer)):
        from .review import submit_review

        return response(submit_review(db, candidate_id, actor=user[0], **body.model_dump()))

    @app.post("/api/v1/uploads")
    def start_upload(body: UploadStart, request: Request, user=Depends(admin)):
        if body.size > settings.max_upload_bytes or Path(body.name).name != body.name:
            raise ValueError("invalid filename or upload size")
        store.check_space(body.size)

        def operation(con):
            identity = uid()
            con.execute(
                """INSERT INTO uploads(id,actor,name,source,expected_size,expected_sha256,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (
                    identity,
                    user[0],
                    body.name,
                    body.source,
                    body.size,
                    body.sha256,
                    Jsonb(body.metadata),
                ),
            )
            return {"upload_id": identity, "offset": 0}

        return response(idempotent(request, user[0], body.model_dump(), operation), 201)

    @app.get("/api/v1/uploads/{upload_id}")
    def upload_status(upload_id: str, user=Depends(admin)):
        rows = db.rows(
            "SELECT id,received,state,expected_size FROM uploads WHERE id=%s AND actor=%s",
            (upload_id, user[0]),
        )
        if not rows:
            raise LookupError()
        return response(rows[0])

    @app.put("/api/v1/uploads/{upload_id}")
    async def upload_chunk(
        upload_id: str, request: Request, offset: int = Query(ge=0), user=Depends(admin)
    ):
        with db.connect() as con:
            row = con.execute(
                "SELECT * FROM uploads WHERE id=%s AND actor=%s FOR UPDATE", (upload_id, user[0])
            ).fetchone()
            if not row:
                raise LookupError()
            if row["state"] != "open" or row["received"] != offset:
                raise FileExistsError("upload offset/state mismatch")
            path = store.path(f"uploads/{upload_id}")
            path.parent.mkdir(exist_ok=True)
            size = 0
            with path.open("r+b" if path.exists() else "w+b") as out:
                out.truncate(offset)
                out.seek(offset)
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > 8 * 1024**2 or offset + size > row["expected_size"]:
                        raise HTTPException(413, "upload chunk exceeds limit")
                    store.check_space(len(chunk))
                    out.write(chunk)
                out.flush()
                __import__("os").fsync(out.fileno())
            con.execute("UPDATE uploads SET received=%s WHERE id=%s", (offset + size, upload_id))
            return response({"upload_id": upload_id, "offset": offset + size})

    @app.post("/api/v1/uploads/{upload_id}/complete")
    def complete_upload(upload_id: str, user=Depends(admin)):
        with db.connect() as con:
            row = con.execute(
                "SELECT * FROM uploads WHERE id=%s AND actor=%s FOR UPDATE", (upload_id, user[0])
            ).fetchone()
            if not row:
                raise LookupError()
            if row["received"] != row["expected_size"]:
                raise FileExistsError("upload incomplete")
            path = store.path(f"uploads/{upload_id}")
            with path.open("rb") as stream:
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual != row["expected_sha256"]:
                raise ValueError("upload SHA-256 mismatch")
            result = register(
                db, store, path, row["source"], row["metadata"], user[0], name=row["name"]
            )
            con.execute("UPDATE uploads SET state='complete' WHERE id=%s", (upload_id,))
            return response(result, 202)

    @app.post("/api/v1/exports")
    def export(body: ExportBody, request: Request, user=Depends(identity)):
        def operation(con):
            release = con.execute("SELECT release_id FROM pointers WHERE kind='quotes'").fetchone()
            if not release:
                raise FileExistsError("no quote release available")
            return {
                "job_id": db.enqueue_on(
                    con,
                    "export",
                    {"release": release["release_id"], "filters": body.filters},
                    user[0],
                )
            }

        return response(idempotent(request, user[0], body.model_dump(), operation), 202)

    @app.get("/api/v1/exports/{job_id}")
    def exported(job_id: str, _user=Depends(identity)):
        rows = db.rows("SELECT state FROM jobs WHERE id=%s AND kind='export'", (job_id,))
        path = store.path(f"exports/{job_id}.parquet")
        if not rows or rows[0]["state"] != "done" or not path.exists():
            raise LookupError()
        return FileResponse(
            path, filename=f"quotes-{job_id}.parquet", media_type="application/vnd.apache.parquet"
        )

    return app
