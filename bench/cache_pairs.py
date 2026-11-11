"""Labeled pairs for choosing the semantic-cache threshold: paraphrases that
should share an answer, and near-misses that must not (same words, different
meaning). Written to bench/cache_pairs.json for gateway/cmd/keel-cachecurve."""

from __future__ import annotations

import json

PARAPHRASES = [
    ("How long do refunds take to arrive?", "How many days until my refund arrives?"),
    ("How long do refunds take to arrive?", "how long does a refund take to arrive"),
    ("What is your return policy?", "Can you explain the return policy?"),
    ("Where is my order?", "Where's my order right now?"),
    ("Can I change my delivery address?", "Is it possible to change the delivery address?"),
    ("Do you ship internationally?", "Do you offer international shipping?"),
    ("How do I cancel my order?", "How can I cancel an order?"),
    ("My package arrived damaged, what do I do?", "What should I do if my package arrived damaged?"),
    ("Is cash on delivery available?", "Can I pay cash on delivery?"),
    ("How do I reset my password?", "How can I reset the password on my account?"),
    ("What payment methods do you accept?", "Which payment methods are accepted?"),
    ("Can I get an invoice for my purchase?", "How do I get an invoice for a purchase?"),
    ("How do I track my shipment?", "How can I track the shipment?"),
    ("Are there any discounts for students?", "Do students get a discount?"),
    ("What are your customer support hours?", "When is customer support available, what hours?"),
    ("Can I exchange an item for a different size?", "Is it possible to exchange an item for another size?"),
    ("How do I apply a coupon code?", "Where do I apply a coupon code?"),
    ("Why was my payment declined?", "My payment was declined, why?"),
    ("How do I delete my account?", "How can I delete my account permanently?"),
    ("Does the warranty cover water damage?", "Is water damage covered by the warranty?"),
    ("My order ORD-10002 arrived damaged, please refund me.", "Please refund me, order ORD-10002 arrived damaged."),
    ("Where is order ORD-10077?", "where is my order ORD-10077"),
]
DIFFERENT = [
    ("How long do refunds take to arrive?", "How long does delivery take to arrive?"),
    ("What is your return policy?", "What is your privacy policy?"),
    ("Where is my order?", "Where is your store?"),
    ("Can I change my delivery address?", "Can I change my email address?"),
    ("Do you ship internationally?", "Do you ship on weekends?"),
    ("How do I cancel my order?", "How do I cancel my subscription?"),
    ("My package arrived damaged, what do I do?", "My package never arrived, what do I do?"),
    ("Is cash on delivery available?", "Is same day delivery available?"),
    ("How do I reset my password?", "How do I reset my router?"),
    ("What payment methods do you accept?", "What return methods do you accept?"),
    ("Can I get an invoice for my purchase?", "Can I get a refund for my purchase?"),
    ("How do I track my shipment?", "How do I return my shipment?"),
    ("Are there any discounts for students?", "Are there any jobs for students?"),
    ("What are your customer support hours?", "What are your warehouse hours?"),
    ("Can I exchange an item for a different size?", "Can I exchange an item for a different colour?"),
    ("How do I apply a coupon code?", "How do I apply for a job?"),
    ("Why was my payment declined?", "Why was my order cancelled?"),
    ("How do I delete my account?", "How do I delete my address?"),
    ("Does the warranty cover water damage?", "Does the warranty cover screen damage?"),
    ("Refund order ORD-12345", "Refund order ORD-54321"),
    # Long texts that differ only in an identifier or an amount: the case the
    # chaos demo exposed (one customer's extraction served to another).
    ("The TV from order ORD-10023 stopped working on day two. I want a refund.",
     "The TV from order ORD-10006 stopped working on day two. I want a refund."),
    ("Hi, my order ORD-10002 arrived damaged. Please refund me.",
     "Hi, my order ORD-10871 arrived damaged. Please refund me."),
    ("Please refund 2,499 rupees for my blender order, it arrived cracked.",
     "Please refund 24,990 rupees for my blender order, it arrived cracked."),
    ("Send the invoice for my last order to asha@example.com please.",
     "Send the invoice for my last order to ravi@example.com please."),
    ("I'd like a refund for ORD-10442, I changed my mind.", "I'd like a refund for ORD-10017, I changed my mind."),
]

if __name__ == "__main__":
    pairs = [{"a": a, "b": b, "same": True} for a, b in PARAPHRASES] + \
            [{"a": a, "b": b, "same": False} for a, b in DIFFERENT]
    json.dump(pairs, open("bench/cache_pairs.json", "w"), indent=1)
    print(f"{len(pairs)} pairs")
