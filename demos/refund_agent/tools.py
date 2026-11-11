"""The refund agent's tools. Each declares its effect:

* get_order, get_refund_status: read-only (effect none)
* issue_refund: the payments API accepts idempotency keys (effect idempotent, risk high)
* send_email: the mailer has no keys but can be searched by the X-Keel-Key
  header, so re-attempts look first (effect reconcile, risk medium)
* add_crm_note: idempotent
"""

from __future__ import annotations

from pydantic import BaseModel

from keel import tool
from keel.tools import ToolRegistry

from .systems import systems

TOOLS = ToolRegistry()


class Order(BaseModel):
    order_id: str
    customer_email: str
    customer_name: str = ""
    amount_inr: int
    status: str
    delivered_at: str | None = None
    refunded: bool = False
    notes: str = ""


class Refund(BaseModel):
    refund_id: str
    order_id: str
    amount_inr: int
    status: str = "pending"


class EmailSent(BaseModel):
    message_id: str
    to: str


@tool(effect="none", risk="low", registry=TOOLS, timeout=10)
async def get_order(order_id: str) -> Order:
    """Look up an order by id (ORD-12345)."""
    return Order.model_validate(await systems().get_order(order_id))


async def _dry_refund(order_id: str, amount_inr: int, reason: str = "", **_: object) -> Refund:
    return Refund(refund_id="rf_dryrun", order_id=order_id, amount_inr=amount_inr, status="dry_run")


@tool(effect="idempotent", risk="high", registry=TOOLS, timeout=10, dry_run=_dry_refund, max_per_run=3)
async def issue_refund(order_id: str, amount_inr: int, reason: str = "", *, key: str) -> Refund:
    """Refund an order to the customer's original payment method. amount_inr is an integer number of rupees."""
    r = await systems().refund(order_id, amount_inr, reason, key)
    return Refund.model_validate(r)


@tool(effect="none", risk="low", registry=TOOLS, timeout=10)
async def get_refund_status(refund_id: str) -> Refund:
    """Check whether a refund has settled."""
    if refund_id == "rf_dryrun":
        return Refund(refund_id=refund_id, order_id="", amount_inr=0, status="settled")
    return Refund.model_validate(await systems().refund_status(refund_id))


async def find_email_by_key(*, key: str, to: str, subject: str, body: str) -> EmailSent | None:
    """Reconciliation: was an email with this key already sent?"""
    found = await systems().find_email(key)
    if found:
        return EmailSent(message_id=found[0]["message_id"], to=found[0]["to"])
    return None


async def _dry_email(to: str, subject: str, body: str, **_: object) -> EmailSent:
    return EmailSent(message_id="msg_dryrun", to=to)


@tool(effect="reconcile", reconcile=find_email_by_key, risk="medium", registry=TOOLS, timeout=10,
      dry_run=_dry_email, max_per_run=3)
async def send_email(to: str, subject: str, body: str, *, key: str) -> EmailSent:
    """Email the customer."""
    return EmailSent.model_validate(await systems().send_email(to, subject, body, key))


async def _dry_note(ticket_id: str, note: str, **_: object) -> dict:
    return {"note_id": "note_dryrun", "ticket_id": ticket_id}


@tool(effect="idempotent", risk="low", registry=TOOLS, timeout=10, dry_run=_dry_note)
async def add_crm_note(ticket_id: str, note: str, *, key: str) -> dict:
    """Record what happened on the ticket in the CRM."""
    return await systems().add_note(ticket_id, note, key)
