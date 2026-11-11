"""The refund agent (e-commerce support), about 8 to 12 steps per run:

1. read the ticket (the customer's text is *user* content)
2. extract order id and request type with a quarantined LLM call (typed schema)
3. get_order
4. check eligibility in plain code (within 30 days of delivery, not refunded,
   the customer's own order) and declare facts for the policy engine
5. the LLM drafts a decision (tool call) and a reply
6. issue_refund: policy says over 5,000 INR needs a human
7. durable sleep (1 hour in production, 1 minute in the demo), then check the
   refund settled
8. send_email with the confirmation (reconcile strategy)
9. add_crm_note

Versions: v1 passes the order system's free-text notes straight to the model
(untrusted, so the run becomes tainted and refunds need approval); v2 runs
them through quarantined extraction first (the dual-LLM pattern); v3 is a
deliberately worse agent that skips the eligibility check, used to prove CI
blocks a regression.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Literal

from keel import Context, PolicyDenied, StepError, workflow
from keel.errors import ExtractionError
from keel.llm import Message, system, trusted, untrusted, user
from keel.tools import tool_result_messages
from pydantic import BaseModel, Field

from .tools import add_crm_note, get_order, get_refund_status, issue_refund, send_email

REFUND_WINDOW_DAYS = 30


class Ticket(BaseModel):
    ticket_id: str
    customer_email: str
    customer_name: str = ""
    message: str
    history: list[str] = Field(default_factory=list, description="earlier turns of the conversation")
    settle_wait_s: float = 60.0
    model: str = "router:support"
    temperature: float = 0.0
    use_kb: bool = False  # give the model the help-center MCP tools


class TicketFields(BaseModel):
    order_id: str | None = Field(default=None, pattern=r"^ORD-\d{5}$")
    request_type: Literal["refund", "return", "status", "other"] = "other"
    reason: Literal["damaged", "late", "not_received", "changed_mind", "other"] = "other"


class NoteFacts(BaseModel):
    delivery_issue: bool = False


class Outcome(BaseModel):
    ticket_id: str
    decision: Literal["refunded", "denied", "rejected", "no_order", "failed"]
    order_id: str | None = None
    amount_inr: int = 0
    refund_id: str | None = None
    email_id: str | None = None
    reasons: list[str] = Field(default_factory=list)
    tainted: bool = False


SYSTEM_PROMPT = """[agent:refund] You are a careful e-commerce support agent.
Policy: refunds only for delivered orders within 30 days, once per order, never more than the order total.
Use the issue_refund tool to refund; never refund an order that is not eligible.
When done, reply with JSON {"reply": "<message to the customer>", "email_to": null}."""


def _eligibility(order: object, fields: TicketFields, ticket: Ticket, now: dt.datetime) -> list[str]:
    reasons: list[str] = []
    if order.customer_email.lower() != ticket.customer_email.lower():  # type: ignore[attr-defined]
        reasons.append("the order belongs to a different customer")
    if order.status != "delivered":  # type: ignore[attr-defined]
        reasons.append(f"the order is {order.status}, not delivered")  # type: ignore[attr-defined]
    elif order.delivered_at:  # type: ignore[attr-defined]
        delivered = dt.datetime.fromisoformat(order.delivered_at)  # type: ignore[attr-defined]
        if delivered.tzinfo is None:
            delivered = delivered.replace(tzinfo=dt.UTC)
        if (now - delivered).days > REFUND_WINDOW_DAYS:
            reasons.append(f"delivered more than {REFUND_WINDOW_DAYS} days ago")
    if order.refunded:  # type: ignore[attr-defined]
        reasons.append("the order was already refunded")
    if fields.request_type not in ("refund", "return"):
        reasons.append("no refund was requested")
    return reasons


async def run_refund(ctx: Context, t: Ticket, *, quarantine_notes: bool, check_eligibility: bool = True) -> Outcome:
    conversation = "\n".join([*t.history, t.message])
    # 2. Quarantined extraction: injected text can at most produce a wrong field.
    try:
        fields = await ctx.extract(TicketFields, conversation, model=t.model)
    except ExtractionError:
        try:  # malformed output: one more try on the large model
            fields = await ctx.extract(TicketFields, conversation, model="router:always-large", key="retry")
        except ExtractionError:
            fields = TicketFields()
    if not fields.order_id:
        email = await ctx.tool(send_email(to=t.customer_email, subject=f"About your request {t.ticket_id}",
                                          body=f"Hi {t.customer_name}, please reply with your order number (it starts with ORD-)."))
        await ctx.tool(add_crm_note(ticket_id=t.ticket_id, note="asked for order id"))
        return Outcome(ticket_id=t.ticket_id, decision="no_order", email_id=email.message_id)

    # 3. Look up the order.
    try:
        order = await ctx.tool(get_order(order_id=fields.order_id))
    except StepError:
        email = await ctx.tool(send_email(to=t.customer_email, subject=f"About your request {t.ticket_id}",
                                          body=f"Hi {t.customer_name}, we could not find order {fields.order_id}."))
        await ctx.tool(add_crm_note(ticket_id=t.ticket_id, note=f"order {fields.order_id} not found"))
        return Outcome(ticket_id=t.ticket_id, decision="no_order", order_id=fields.order_id, email_id=email.message_id)

    # 4. Eligibility is plain code; the policy engine gets it as facts.
    reasons = _eligibility(order, fields, t, ctx.now()) if check_eligibility else []
    eligible = not reasons
    if check_eligibility:
        ctx.set_fact("order_id", order.order_id)
        ctx.set_fact("order_total_inr", order.amount_inr)
        ctx.set_fact("eligible", eligible)
    facts = {"order_id": order.order_id, "eligible": eligible, "amount_inr": order.amount_inr,
             "reasons": reasons, "reason": fields.reason, "customer_name": t.customer_name or order.customer_name,
             "customer_email": t.customer_email}
    messages: list[Message] = [system(SYSTEM_PROMPT), trusted("FACTS: " + json.dumps(facts)), user(conversation)]
    if order.notes:
        if quarantine_notes:
            try:
                nf = await ctx.extract(NoteFacts, order.notes, model=t.model)
                facts["delivery_issue"] = nf.delivery_issue
                messages[1] = trusted("FACTS: " + json.dumps(facts))
            except ExtractionError:
                pass
        else:
            messages.append(untrusted("Order notes from the order system: " + order.notes, source="order_notes"))

    # 5-6. The model decides; tool calls go through validation, policy and approval.
    refund = None
    draft: dict = {}
    blocked = False
    tools: list = [issue_refund]
    if t.use_kb:
        tools += [s for s in ctx.registered_tools(source="mcp") if s.name.startswith("kb_")]
    for _turn in range(min(4, ctx.limits.max_turns)):
        reply = await ctx.llm(messages, model=t.model, tools=tools, temperature=t.temperature,
                              response_format={"type": "json_object"})
        if reply.tool_calls:
            results = await ctx.tools(reply.tool_calls)
            for r in results:
                if r.name == "issue_refund":
                    if r.ok:
                        refund = r.output
                    elif r.error_type in ("PolicyDenied", "ApprovalRejected"):
                        blocked = True
            messages += [reply.message, *tool_result_messages(reply.tool_calls, results)]
            continue
        try:
            draft = reply.parsed()
        except ValueError:
            draft = {"reply": reply.text or ""}
        break

    # 7. Wait for settlement durably (the worker is freed meanwhile).
    if refund is not None:
        await ctx.sleep(seconds=t.settle_wait_s)
        status = await ctx.tool(get_refund_status(refund_id=refund["refund_id"]))
        if status.status == "pending":
            await ctx.sleep(seconds=t.settle_wait_s)
            await ctx.tool(get_refund_status(refund_id=refund["refund_id"]))

    # 8. Confirmation email: output checked; the recipient is policy-guarded.
    name = t.customer_name or order.customer_name
    body = ctx.check_output(str(draft.get("reply") or f"Hi {name}, we have reviewed your request."),
                            allowed_emails=[t.customer_email])
    subject = f"Your request {t.ticket_id}"
    to = draft.get("email_to") or t.customer_email
    try:
        email = await ctx.tool(send_email(to=to, subject=subject, body=body))
    except PolicyDenied:
        email = await ctx.tool(send_email(to=t.customer_email, subject=subject, body=body), key="customer")

    # 9. CRM note.
    decision = "refunded" if refund else ("rejected" if blocked else "denied")
    await ctx.tool(add_crm_note(ticket_id=t.ticket_id,
                                note=f"decision={decision} order={order.order_id} refund={refund and refund['amount_inr']}"))
    return Outcome(ticket_id=t.ticket_id, decision=decision, order_id=order.order_id,
                   amount_inr=refund["amount_inr"] if refund else 0, refund_id=refund and refund["refund_id"],
                   email_id=email.message_id, reasons=reasons, tainted=ctx.tainted)


@workflow(name="refund_agent", version="1", max_steps=60)
async def refund_agent_v1(ctx: Context, t: Ticket) -> Outcome:
    return await run_refund(ctx, t, quarantine_notes=False)


@workflow(name="refund_agent", version="2", max_steps=60)
async def refund_agent_v2(ctx: Context, t: Ticket) -> Outcome:
    return await run_refund(ctx, t, quarantine_notes=True)


@workflow(name="refund_agent", version="3", max_steps=60)
async def refund_agent_v3_bad(ctx: Context, t: Ticket) -> Outcome:
    """Deliberately worse: trusts the model instead of checking eligibility."""
    return await run_refund(ctx, t, quarantine_notes=True, check_eligibility=False)


ALL = [refund_agent_v1, refund_agent_v2, refund_agent_v3_bad]
