"""Mock systems for the demos, each a small FastAPI app over SQLite:

* orders   read-only order data (customer, amount in INR, delivery date, notes)
* payments refunds with idempotency keys (UNIQUE constraint in the ledger)
* email    no idempotency support, but messages are searchable by X-Keel-Key
* crm      notes with idempotency keys
* web      a tiny search engine + pages for the research agent (some pages
           carry prompt-injection text)

Run one with:  KEEL_MOCK=payments uvicorn demos.mocks.services:app --port 9002
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from .chaos import Chaos

DATA_DIR = os.environ.get("KEEL_MOCK_DATA", "./data/mocks")


class DB:
    def __init__(self, name: str, schema: str):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.path = os.path.join(DATA_DIR, f"{name}.sqlite")
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self.schema = schema
        self.conn.executescript(schema)

    def q(self, sql: str, args: tuple = ()) -> list[dict]:
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def x(self, sql: str, args: tuple = ()) -> int:
        with self.lock:
            cur = self.conn.execute(sql, args)
            return cur.lastrowid or 0

    def reset(self, tables: list[str]) -> None:
        with self.lock:
            for t in tables:
                self.conn.execute(f"DELETE FROM {t}")


def _admin(app: FastAPI, chaos: Chaos, reset: Any) -> None:
    @app.post("/admin/chaos")
    async def set_chaos(cfg: dict) -> dict:
        return chaos.configure(**cfg)

    @app.post("/admin/reset")
    async def do_reset() -> dict:
        reset()
        chaos.reset()
        return {"ok": True}

    @app.get("/admin/chaos")
    async def get_chaos() -> dict:
        return {"config": chaos.cfg.__dict__, "stats": chaos.stats}

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}


# ---------------------------------------------------------------------- orders


def orders_app() -> FastAPI:
    app = FastAPI(title="mock-orders")
    db = DB("orders", """
        CREATE TABLE IF NOT EXISTS orders (
          order_id TEXT PRIMARY KEY, customer_email TEXT, customer_name TEXT, items TEXT,
          amount_inr INTEGER, status TEXT, delivered_at TEXT, refunded INTEGER DEFAULT 0, notes TEXT);""")
    chaos = Chaos()
    _admin(app, chaos, lambda: db.reset(["orders"]))

    @app.post("/admin/seed")
    async def seed(body: dict) -> dict:
        rows = body.get("orders", [])
        with db.lock:
            db.conn.execute("BEGIN")
            for o in rows:
                db.conn.execute("INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?,?,?,?)", (
                    o["order_id"], o["customer_email"], o.get("customer_name", ""), json.dumps(o.get("items", [])),
                    int(o["amount_inr"]), o.get("status", "delivered"), o.get("delivered_at"), int(o.get("refunded", 0)),
                    o.get("notes", "")))
            db.conn.execute("COMMIT")
        return {"seeded": len(rows)}

    @app.get("/orders/{order_id}")
    async def get_order(order_id: str) -> dict:
        await chaos.before()
        rows = db.q("SELECT * FROM orders WHERE order_id = ?", (order_id,))
        if not rows:
            raise HTTPException(404, "no such order")
        o = rows[0]
        o["items"] = json.loads(o["items"] or "[]")
        o["refunded"] = bool(o["refunded"])
        return o

    @app.get("/admin/orders")
    async def all_orders() -> list[dict]:
        return db.q("SELECT order_id, customer_email, amount_inr, status, delivered_at FROM orders")

    return app


# ---------------------------------------------------------------------- payments


class RefundReq(BaseModel):
    order_id: str
    amount_inr: int
    reason: str = ""


def payments_app() -> FastAPI:
    app = FastAPI(title="mock-payments")
    db = DB("payments", """
        CREATE TABLE IF NOT EXISTS refunds (
          refund_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, amount_inr INTEGER NOT NULL,
          idempotency_key TEXT NOT NULL UNIQUE, reason TEXT, created_at REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS refunds_order ON refunds(order_id);""")
    chaos = Chaos()
    settle_after = float(os.environ.get("KEEL_MOCK_SETTLE_S", "2"))
    _admin(app, chaos, lambda: db.reset(["refunds"]))

    def view(r: dict) -> dict:
        r = dict(r)
        r["status"] = "settled" if time.time() - r["created_at"] >= settle_after else "pending"
        return r

    @app.post("/refunds", status_code=201)
    async def refund(req: RefundReq, idempotency_key: str = Header(...)) -> dict:
        await chaos.before()
        if req.amount_inr <= 0:
            raise HTTPException(400, "amount must be positive")
        existing = db.q("SELECT * FROM refunds WHERE idempotency_key = ?", (idempotency_key,))
        if existing:
            return {**view(existing[0]), "replayed": True}
        rid = "rf_" + uuid.uuid4().hex[:12]
        try:
            db.x("INSERT INTO refunds VALUES (?,?,?,?,?,?)",
                 (rid, req.order_id, req.amount_inr, idempotency_key, req.reason, time.time()))
        except sqlite3.IntegrityError:  # concurrent duplicate: the unique key wins
            r = db.q("SELECT * FROM refunds WHERE idempotency_key = ?", (idempotency_key,))[0]
            return {**view(r), "replayed": True}
        row = db.q("SELECT * FROM refunds WHERE refund_id = ?", (rid,))[0]
        await chaos.after_commit()
        return view(row)

    @app.get("/refunds/{refund_id}")
    async def get_refund(refund_id: str) -> dict:
        await chaos.before()
        rows = db.q("SELECT * FROM refunds WHERE refund_id = ?", (refund_id,))
        if not rows:
            raise HTTPException(404, "no such refund")
        return view(rows[0])

    @app.get("/refunds")
    async def by_order(order_id: str) -> list[dict]:
        return [view(r) for r in db.q("SELECT * FROM refunds WHERE order_id = ?", (order_id,))]

    @app.get("/admin/ledger")
    async def ledger() -> list[dict]:
        return db.q("SELECT refund_id, order_id, amount_inr, idempotency_key, created_at FROM refunds")

    return app


# ---------------------------------------------------------------------- email


class EmailReq(BaseModel):
    to: str
    subject: str
    body: str


def email_app() -> FastAPI:
    app = FastAPI(title="mock-email")
    db = DB("email", """
        CREATE TABLE IF NOT EXISTS messages (
          message_id TEXT PRIMARY KEY, to_addr TEXT, subject TEXT, body TEXT, keel_key TEXT, created_at REAL);
        CREATE INDEX IF NOT EXISTS messages_key ON messages(keel_key);""")
    chaos = Chaos()
    _admin(app, chaos, lambda: db.reset(["messages"]))

    @app.post("/messages", status_code=201)
    async def send(req: EmailReq, x_keel_key: str | None = Header(default=None)) -> dict:
        # No idempotency: every accepted request sends another email.
        await chaos.before()
        mid = "msg_" + uuid.uuid4().hex[:12]
        db.x("INSERT INTO messages VALUES (?,?,?,?,?,?)", (mid, req.to, req.subject, req.body, x_keel_key, time.time()))
        await chaos.after_commit()
        return {"message_id": mid, "to": req.to}

    @app.get("/messages")
    async def search(key: str) -> list[dict]:
        await chaos.before()
        return db.q("SELECT message_id, to_addr AS 'to', subject, keel_key, created_at FROM messages WHERE keel_key = ?",
                    (key,))

    @app.get("/admin/messages")
    async def all_messages() -> list[dict]:
        return db.q("SELECT message_id, to_addr AS 'to', subject, body, keel_key, created_at FROM messages")

    return app


# ---------------------------------------------------------------------- crm


class NoteReq(BaseModel):
    ticket_id: str
    note: str


def crm_app() -> FastAPI:
    app = FastAPI(title="mock-crm")
    db = DB("crm", """
        CREATE TABLE IF NOT EXISTS notes (
          note_id TEXT PRIMARY KEY, ticket_id TEXT, note TEXT, idempotency_key TEXT UNIQUE, created_at REAL);""")
    chaos = Chaos()
    _admin(app, chaos, lambda: db.reset(["notes"]))

    @app.post("/notes", status_code=201)
    async def add(req: NoteReq, idempotency_key: str = Header(...)) -> dict:
        await chaos.before()
        ex = db.q("SELECT * FROM notes WHERE idempotency_key = ?", (idempotency_key,))
        if ex:
            return ex[0]
        nid = "note_" + uuid.uuid4().hex[:10]
        try:
            db.x("INSERT INTO notes VALUES (?,?,?,?,?)", (nid, req.ticket_id, req.note, idempotency_key, time.time()))
        except sqlite3.IntegrityError:
            return db.q("SELECT * FROM notes WHERE idempotency_key = ?", (idempotency_key,))[0]
        await chaos.after_commit()
        return db.q("SELECT * FROM notes WHERE note_id = ?", (nid,))[0]

    @app.get("/admin/notes")
    async def notes() -> list[dict]:
        return db.q("SELECT * FROM notes")

    return app


# ---------------------------------------------------------------------- web (research)


def web_app() -> FastAPI:
    from .corpus import PAGES, search

    app = FastAPI(title="mock-web")
    chaos = Chaos()
    _admin(app, chaos, lambda: None)

    @app.get("/search")
    async def do_search(q: str, n: int = 4) -> list[dict]:
        await chaos.before()
        return search(q, n)

    @app.get("/pages/{page_id}")
    async def page(page_id: str) -> dict:
        await chaos.before()
        if page_id not in PAGES:
            raise HTTPException(404, "no such page")
        return PAGES[page_id]

    # The agent's vector memory: notes embedded with the gateway's hashed n-gram embedding.
    memory = DB("memory", """
        CREATE TABLE IF NOT EXISTS notes (key TEXT PRIMARY KEY, namespace TEXT, text TEXT, created_at REAL);
        CREATE TABLE IF NOT EXISTS reports (key TEXT PRIMARY KEY, report_id TEXT, title TEXT, body TEXT, created_at REAL);""")

    @app.post("/memory")
    async def remember(body: dict, idempotency_key: str = Header(...)) -> dict:
        memory.x("INSERT OR IGNORE INTO notes VALUES (?,?,?,?)",
                 (idempotency_key, body["namespace"], body["text"], time.time()))
        return {"stored": True, "key": idempotency_key}

    @app.get("/memory/search")
    async def recall(namespace: str, q: str, n: int = 5) -> list[dict]:
        rows = memory.q("SELECT text FROM notes WHERE namespace = ?", (namespace,))
        qw = set(q.lower().split())
        rows.sort(key=lambda r: -len(qw & set(r["text"].lower().split())))
        return rows[:n]

    @app.post("/reports", status_code=201)
    async def publish(body: dict, idempotency_key: str = Header(...)) -> dict:
        ex = memory.q("SELECT report_id, title FROM reports WHERE key = ?", (idempotency_key,))
        if ex:
            return ex[0]
        rid = "rep_" + uuid.uuid4().hex[:10]
        memory.x("INSERT OR IGNORE INTO reports VALUES (?,?,?,?,?)",
                 (idempotency_key, rid, body["title"], body["body"], time.time()))
        return memory.q("SELECT report_id, title FROM reports WHERE key = ?", (idempotency_key,))[0]

    @app.get("/admin/reports")
    async def reports() -> list[dict]:
        return memory.q("SELECT report_id, title, length(body) AS chars, created_at FROM reports")

    return app


def _make(kind: str) -> FastAPI:
    return {"orders": orders_app, "payments": payments_app, "email": email_app, "crm": crm_app,
            "web": web_app}[kind]()


def combined_app() -> FastAPI:
    """All mocks behind one port under /orders-svc, /payments-svc, ... (dev convenience)."""
    app = FastAPI(title="mocks")
    for kind in ("orders", "payments", "email", "crm", "web"):
        app.mount(f"/{kind}", _make(kind))
    return app


_kind = os.environ.get("KEEL_MOCK", "combined")
app = combined_app() if _kind == "combined" else _make(_kind)


def _tracer() -> Any:
    from keel import telemetry
    telemetry.setup_tracing(f"keel-mock-{_kind}")
    return telemetry._tracer


_TRACER = _tracer() if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") else None


@app.middleware("http")
async def _trace_and_no_cache(request: Request, call_next: Any) -> Any:
    """Join the caller's trace (W3C traceparent) so one trace covers the
    server, worker, gateway, sandbox and the services the tools call."""
    if _TRACER is None or request.url.path.startswith(("/health", "/admin")):
        resp = await call_next(request)
    else:
        from opentelemetry.propagate import extract
        with _TRACER.start_as_current_span(f"{request.method} {request.url.path}", context=extract(dict(request.headers))):
            resp = await call_next(request)
    resp.headers["Cache-Control"] = "no-store"
    return resp
