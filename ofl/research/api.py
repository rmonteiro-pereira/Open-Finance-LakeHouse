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
from fastapi.responses import FileResponse, JSONResponse, Response
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


class LibrarySyncBody(BaseModel):
    kind: Literal["news", "letters", "macro"]
    limit: int = Field(20, ge=1, le=200)
    collection: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]{1,80}$")


class WorkspaceBody(BaseModel):
    kind: Literal["investigation", "portfolio", "dataset", "watchlist", "check"]
    title: str = Field(min_length=1, max_length=200)
    content: dict
    expected_revision: int = Field(0, ge=0)


class ScheduleBody(BaseModel):
    interval_minutes: int = Field(60, ge=15, le=10080)
    enabled: bool = True


class FeedbackBody(BaseModel):
    category: Literal[
        "wrong_source", "wrong_number", "missing_context", "coverage", "usability", "other"
    ]
    detail: str = Field(min_length=1, max_length=4000)
    artifact_id: str | None = Field(None, max_length=128)


class FeedbackReview(BaseModel):
    expected_revision: int = Field(0, ge=0)
    state: Literal["triaged", "evaluation_candidate", "resolved", "dismissed"]
    note: str = Field(min_length=1, max_length=2000)


def authenticate(settings, headers, method):
    """Resolve (actor, role) from gateway identity or a read-only service bearer token."""
    authorization = headers.get("authorization", "")
    machine = authorization.startswith("Bearer ")
    if not machine:
        token = headers.get("x-ofl-gateway-token", "")
        if not settings.gateway_token or not hmac.compare_digest(
            token.encode(), settings.gateway_token.encode()
        ):
            raise HTTPException(401, "verified private gateway required")
    try:
        roles = json.loads(settings.roles)
        service_tokens = json.loads(settings.service_tokens)
    except (ValueError, TypeError):
        raise HTTPException(503, "research access is not configured") from None
    if machine:
        supplied = authorization[7:].encode()
        actor = None
        if isinstance(service_tokens, dict):
            for principal, secret in service_tokens.items():
                if (
                    isinstance(secret, str)
                    and len(secret) >= 32
                    and hmac.compare_digest(supplied, secret.encode())
                ):
                    actor = principal
        if actor is None:
            raise HTTPException(401, "invalid service credential")
        if method not in {"GET", "HEAD"}:
            raise HTTPException(403, "service credentials are read-only")
    else:
        actor = headers.get("x-ofl-identity", "")
    # An absent or empty identity never matches a role, even one configured for "".
    role = roles.get(actor) if actor and isinstance(roles, dict) else None
    if role not in {"reader", "reviewer", "admin"}:
        raise HTTPException(403, "identity has no research access")
    if not machine and method not in {"GET", "HEAD"}:
        if headers.get("origin") != settings.origin:
            raise HTTPException(403, "same-origin request required")
    return actor, "reader" if machine else role


def require_admin(user):
    if user[1] != "admin":
        raise HTTPException(403, "administrator role required")
    return user


def require_reviewer(user):
    if user[1] not in {"reviewer", "admin"}:
        raise HTTPException(403, "reviewer role required")
    return user


def can_read_feedback(owner, user):
    """Feedback history is visible to its author and to reviewers or admins."""
    return owner == user[0] or user[1] in {"reviewer", "admin"}


def create_app(settings=None):
    settings = settings or ResearchSettings()
    db = Database(settings)
    store = Store.from_settings(settings)
    app = FastAPI(title="OFL private research", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.db, app.state.store = db, store

    def identity(request: Request):
        return authenticate(settings, request.headers, request.method)

    def admin(user=Depends(identity)):
        return require_admin(user)

    def reviewer(user=Depends(identity)):
        return require_reviewer(user)

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

    @app.get("/api/v1/openapi", include_in_schema=False)
    def private_schema(_user=Depends(identity)):
        schema = app.openapi()
        schema.setdefault("components", {})["securitySchemes"] = {
            "ResearchBearer": {"type": "http", "scheme": "bearer"},
            "GatewayToken": {"type": "apiKey", "in": "header", "name": "X-OFL-Gateway-Token"},
            "GatewayIdentity": {"type": "apiKey", "in": "header", "name": "X-OFL-Identity"},
        }
        schema["security"] = [{"ResearchBearer": []}, {"GatewayToken": [], "GatewayIdentity": []}]
        return response(schema)

    @app.get("/api/v1/releases")
    def releases(limit: int = Query(30, ge=1, le=100), _user=Depends(identity)):
        return response(
            db.rows(
                "SELECT id,kind,delta_version,metadata,created_at FROM releases ORDER BY created_at DESC,id LIMIT %s",
                (limit,),
            )
        )

    @app.get("/api/v1/workbench")
    def workbench(_user=Depends(identity)):
        from .search import index_coverage

        counts = db.rows(
            "SELECT source,item_type,count(*) AS items FROM items GROUP BY source,item_type"
        )
        macro = db.rows(
            "SELECT s.id,s.generated_at,s.catalog FROM macro_pointer p JOIN macro_snapshots s ON s.id=p.snapshot_id"
        )
        dataset = None
        if macro:
            dataset = {
                "snapshot_id": macro[0]["id"],
                "generated_at": macro[0]["generated_at"],
                "series_with_data": sum(
                    row.get("redistribution") == "open" and bool(row.get("rows"))
                    for row in macro[0]["catalog"]["series"]
                ),
            }
        return response(
            {
                "documents": counts,
                "index_coverage": index_coverage(db),
                "macro": dataset,
                "processing": db.rows("SELECT state,count(*) AS assets FROM assets GROUP BY state"),
                "date_quality": db.rows(
                    "SELECT source,date_precision,count(*) AS items FROM items GROUP BY source,date_precision"
                ),
                "qualification": "Indexed scope only; collection totals and source coverage are separate.",
            }
        )

    @app.post("/api/v1/library/sync")
    def library_sync(body: LibrarySyncBody, request: Request, user=Depends(admin)):
        payload = body.model_dump()

        def operation(con):
            return {"job_id": db.enqueue_on(con, "library_sync", payload, user[0])}

        return response(idempotent(request, user[0], payload, operation), 202)

    @app.get("/api/v1/data/catalog")
    def data_catalog(
        snapshot_id: str | None = None,
        q: str | None = Query(None, max_length=200),
        _user=Depends(identity),
    ):
        from .macro import catalog

        return response(catalog(db, snapshot_id=snapshot_id, query=q))

    @app.get("/api/v1/data/series/{series_id}")
    def macro_series(
        series_id: str,
        snapshot_id: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = Query(1000, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        _user=Depends(identity),
    ):
        from .macro import series_query

        return response(
            series_query(
                db,
                store,
                series_id,
                snapshot_id=snapshot_id,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                offset=offset,
            )
        )

    @app.get("/api/v1/data/series/{series_id}/export")
    def macro_export(
        series_id: str,
        snapshot_id: str,
        date_from: date | None = None,
        date_to: date | None = None,
        _user=Depends(identity),
    ):
        from .macro import IDENTIFIER, export_series

        if not IDENTIFIER.fullmatch(snapshot_id):
            raise ValueError("invalid snapshot ID")
        content, data = export_series(
            db, store, series_id, snapshot_id=snapshot_id, date_from=date_from, date_to=date_to
        )
        return Response(
            content,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="ofl-{series_id}-{snapshot_id}.csv"',
                "X-OFL-Snapshot": data["snapshot_id"],
            },
        )

    @app.get("/api/v1/data/series/{series_id}/manifest")
    def macro_manifest(
        series_id: str,
        snapshot_id: str,
        date_from: date | None = None,
        date_to: date | None = None,
        _user=Depends(identity),
    ):
        from .macro import export_series

        content, data = export_series(
            db, store, series_id, snapshot_id=snapshot_id, date_from=date_from, date_to=date_to
        )
        data.pop("rows")
        data["csv_sha256"] = hashlib.sha256(content.encode()).hexdigest()
        data["csv_columns"] = [
            "series_id",
            "observation_date",
            "value",
            "unit",
            "frequency",
            "snapshot_id",
        ]
        return response(data)

    @app.get("/api/v1/passages/{passage_id}")
    def passage(passage_id: str, _user=Depends(identity)):
        rows = db.rows(
            """SELECT p.*,i.title,i.source,i.collection,i.date_start,i.date_end,i.date_precision,
            i.metadata->>'url' AS source_url,i.metadata->>'content_tier' AS content_tier,
            i.metadata->>'epistemic_status' AS epistemic_status FROM passages p JOIN items i ON i.id=p.item_id WHERE p.id=%s
            AND (i.source<>'news' OR i.metadata->>'content_tier'<>'headline_only' OR p.active)""",
            (passage_id,),
        )
        if not rows:
            raise LookupError()
        return response(
            {
                "passage": rows[0],
                "qualification": "A superseded passage remains citable with active=false; source text is not reviewed numerical truth.",
            }
        )

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
                "pipeline_enabled": bool(settings.airflow_url and settings.airflow_token),
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
        rows = db.rows(
            """SELECT a.* FROM assets a JOIN items i ON i.id=a.item_id WHERE a.id=%s
            AND (i.source<>'news' OR i.metadata->>'content_tier'<>'headline_only' OR a.active)""",
            (asset_id,),
        )
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
        if body.action == "pipeline" and not (settings.airflow_url and settings.airflow_token):
            raise HTTPException(503, "Airflow action integration is not configured")
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

    from .analytics import DatasetSpec

    @app.get("/api/v1/datasets/preview")
    def dataset_preview(specification: str = Query(max_length=8000), _user=Depends(identity)):
        from .analytics import prepare_dataset

        data = prepare_dataset(db, store, DatasetSpec.model_validate_json(specification))
        return response({"manifest": data["manifest"], "preview": data["preview"]})

    @app.post("/api/v1/datasets/prepare")
    def dataset_prepare(body: DatasetSpec, _user=Depends(identity)):
        from .analytics import prepare_dataset

        data = prepare_dataset(db, store, body)
        return response({"manifest": data["manifest"], "preview": data["preview"]})

    @app.post("/api/v1/datasets/export")
    def dataset_export(body: DatasetSpec, _user=Depends(identity)):
        from .analytics import prepare_dataset

        data = prepare_dataset(db, store, body)
        return Response(
            data["csv"],
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="ofl-dataset.csv"'},
        )

    @app.post("/api/v1/datasets/bundle")
    def dataset_bundle(body: DatasetSpec, _user=Depends(identity)):
        from .dataset_bundle import bundle

        return Response(
            bundle(db, store, body),
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="ofl-dataset-reproduction.zip"',
                     "Cache-Control": "no-store"},
        )

    @app.get("/api/v1/datasets/bundle")
    def dataset_bundle_read(specification: str = Query(max_length=8000), _user=Depends(identity)):
        # Read-only service credentials can export the same artifact; no saved state.
        return dataset_bundle(DatasetSpec.model_validate_json(specification), _user)

    @app.get("/api/v1/quotes/compare")
    def comparison(
        symbol: str = Query(min_length=1, max_length=32),
        market: str = Query(pattern=r"^\d{3}$"),
        date_from: date = Query(),
        date_to: date = Query(),
        release: str = Query(max_length=128),
        _user=Depends(identity),
    ):
        from .analytics import compare_quotes

        return response(
            compare_quotes(
                db,
                symbol=symbol,
                market=market,
                date_from=date_from,
                date_to=date_to,
                release=release,
            )
        )

    @app.get("/api/v1/releases/diff")
    def release_comparison(
        left: str = Query(max_length=128),
        right: str = Query(max_length=128),
        symbol: str = Query(min_length=1, max_length=32),
        market: str = Query(pattern=r"^\d{3}$"),
        date_from: date = Query(),
        date_to: date = Query(),
        _user=Depends(identity),
    ):
        from .analytics import release_diff

        return response(
            release_diff(
                db,
                left=left,
                right=right,
                symbol=symbol,
                market=market,
                date_from=date_from,
                date_to=date_to,
            )
        )

    @app.get("/api/v1/workspaces")
    def workspace_list(kind: str | None = Query(None, max_length=30), user=Depends(identity)):
        return response(
            db.rows(
                "SELECT id,kind,title,current_revision,created_at FROM workspaces WHERE owner=%s AND (%s::text IS NULL OR kind=%s) ORDER BY created_at DESC LIMIT 200",
                (user[0], kind, kind),
            )
        )

    @app.post("/api/v1/workspaces")
    def workspace_create(body: WorkspaceBody, user=Depends(identity)):
        from .workspaces import save

        return response(
            save(
                db,
                user[0],
                body.kind,
                body.title,
                body.content,
                expected_revision=body.expected_revision,
            ),
            201,
        )

    @app.post("/api/v1/workspaces/{workspace_id}")
    def workspace_save(workspace_id: str, body: WorkspaceBody, user=Depends(identity)):
        from .workspaces import save

        return response(
            save(
                db,
                user[0],
                body.kind,
                body.title,
                body.content,
                workspace_id=workspace_id,
                expected_revision=body.expected_revision,
            )
        )

    @app.get("/api/v1/workspaces/{workspace_id}")
    def workspace_read(
        workspace_id: str, revision: int | None = Query(None, ge=1), user=Depends(identity)
    ):
        from .workspaces import load

        return response(load(db, user[0], workspace_id, revision))

    @app.get("/api/v1/workspaces/{workspace_id}/revisions")
    def workspace_history(workspace_id: str, user=Depends(identity)):
        from .workspaces import load

        load(db, user[0], workspace_id)
        return response(
            db.rows(
                "SELECT revision,sha256,created_at FROM workspace_revisions WHERE workspace_id=%s ORDER BY revision DESC LIMIT 200",
                (workspace_id,),
            )
        )

    @app.get("/api/v1/workspaces/{workspace_id}/export")
    def workspace_export(
        workspace_id: str,
        revision: int = Query(ge=1),
        format: Literal["json", "markdown"] = "json",
        user=Depends(identity),
    ):
        from .workspaces import export_markdown, load

        data = load(db, user[0], workspace_id, revision)
        if format == "markdown":
            return Response(
                export_markdown(data),
                media_type="text/markdown",
                headers={"Content-Disposition": 'attachment; filename="ofl-research.md"'},
            )
        return response(data)

    @app.post("/api/v1/workspaces/{workspace_id}/run")
    def workspace_run(workspace_id: str, user=Depends(identity)):
        from .workspaces import rerun

        return response(rerun(db, store, user[0], workspace_id))

    @app.get("/api/v1/workspaces/{workspace_id}/diff")
    def workspace_diff(
        workspace_id: str, left: int = Query(ge=1), right: int = Query(ge=1), user=Depends(identity)
    ):
        from .workspaces import load

        a, b = load(db, user[0], workspace_id, left), load(db, user[0], workspace_id, right)
        old, new = a["body"]["content"], b["body"]["content"]
        return response(
            {
                "left": {"revision": left, "sha256": a["sha256"]},
                "right": {"revision": right, "sha256": b["sha256"]},
                "changed_fields": [
                    k for k in sorted(old.keys() | new.keys()) if old.get(k) != new.get(k)
                ],
                "before": old,
                "after": new,
            }
        )

    @app.post("/api/v1/feedback")
    def feedback_create(body: FeedbackBody, user=Depends(identity)):
        if body.artifact_id:
            from .workspaces import load

            load(db, user[0], body.artifact_id)
        feedback_id = uid()
        with db.connect() as con:
            con.execute(
                "INSERT INTO research_feedback(id,owner,category,detail,artifact_id) VALUES (%s,%s,%s,%s,%s)",
                (feedback_id, user[0], body.category, body.detail, body.artifact_id),
            )
        return response({"id": feedback_id, "state": "new"}, 201)

    @app.get("/api/v1/feedback")
    def feedback_list(user=Depends(identity)):
        if user[1] in {"reviewer", "admin"}:
            return response(
                db.rows("SELECT * FROM research_feedback ORDER BY created_at DESC LIMIT 200")
            )
        return response(
            db.rows(
                "SELECT * FROM research_feedback WHERE owner=%s ORDER BY created_at DESC LIMIT 200",
                (user[0],),
            )
        )

    @app.post("/api/v1/feedback/{feedback_id}/review")
    def feedback_review(feedback_id: str, body: FeedbackReview, user=Depends(reviewer)):
        with db.connect() as con:
            item = con.execute(
                "SELECT * FROM research_feedback WHERE id=%s FOR UPDATE", (feedback_id,)
            ).fetchone()
            if not item:
                raise LookupError("feedback not found")
            current = con.execute(
                "SELECT COALESCE(max(revision),0) AS revision FROM feedback_reviews WHERE feedback_id=%s",
                (feedback_id,),
            ).fetchone()["revision"]
            if current != body.expected_revision:
                raise FileExistsError("feedback review changed")
            con.execute(
                "INSERT INTO feedback_reviews(feedback_id,revision,actor,state,note) VALUES (%s,%s,%s,%s,%s)",
                (feedback_id, current + 1, user[0], body.state, body.note),
            )
            con.execute(
                "UPDATE research_feedback SET state=%s WHERE id=%s", (body.state, feedback_id)
            )
        return response({"id": feedback_id, "revision": current + 1, "state": body.state})

    @app.get("/api/v1/feedback/{feedback_id}/history")
    def feedback_history(feedback_id: str, user=Depends(identity)):
        items = db.rows("SELECT owner FROM research_feedback WHERE id=%s", (feedback_id,))
        if not items or not can_read_feedback(items[0]["owner"], user):
            raise LookupError("feedback not found")
        return response(
            db.rows(
                "SELECT revision,actor,state,note,created_at FROM feedback_reviews WHERE feedback_id=%s ORDER BY revision",
                (feedback_id,),
            )
        )

    @app.post("/api/v1/workspaces/{workspace_id}/schedule")
    def check_schedule(workspace_id: str, body: ScheduleBody, user=Depends(identity)):
        from .workspaces import owned

        with db.connect() as con:
            item = owned(con, user[0], workspace_id)
            if item["kind"] != "check":
                raise ValueError("only saved checks can be scheduled")
            con.execute(
                "INSERT INTO check_schedules(workspace_id,owner,interval_minutes,enabled) VALUES (%s,%s,%s,%s) ON CONFLICT(workspace_id) DO UPDATE SET interval_minutes=excluded.interval_minutes,enabled=excluded.enabled,next_run_at=now()",
                (workspace_id, user[0], body.interval_minutes, body.enabled),
            )
        return response(
            {
                "workspace_id": workspace_id,
                "interval_minutes": body.interval_minutes,
                "enabled": body.enabled,
            }
        )

    @app.get("/api/v1/workspaces/{workspace_id}/schedule")
    def schedule_read(workspace_id: str, user=Depends(identity)):
        from .workspaces import load

        load(db, user[0], workspace_id)
        return response(
            db.rows(
                "SELECT interval_minutes,enabled,next_run_at FROM check_schedules WHERE workspace_id=%s",
                (workspace_id,),
            )
        )

    @app.get("/api/v1/jobs/{job_id}/events")
    def job_events(job_id: str, _user=Depends(identity)):
        return response(
            db.rows(
                "SELECT state,detail,created_at FROM job_events WHERE job_id=%s ORDER BY id LIMIT 500",
                (job_id,),
            )
        )

    return app
