import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, Approval, short } from "../api";

function Card({ a, onDone }: { a: Approval; onDone: () => void }) {
  const [edits, setEdits] = useState(JSON.stringify(a.action.args, null, 2));
  const [comment, setComment] = useState("");
  const [err, setErr] = useState("");
  const decide = async (decision: string, withEdits: boolean) => {
    try {
      await api.decide(a.approval_id, decision, withEdits ? JSON.parse(edits) : undefined, comment);
      onDone();
    } catch (e) { setErr(String(e)); }
  };
  return (
    <div className="card">
      <div className="cardhead">
        <b>{a.action.tool}</b> <span className={`tag ${a.action.risk === "high" ? "untrusted" : ""}`}>{a.action.risk} risk</span>
        <Link to={`/runs/${a.run_id}`} className="mono">run {short(a.run_id)}</Link>
        <span className="dim">{new Date(a.created_at).toLocaleString()}</span>
        {a.expires_at && <span className="dim">expires {new Date(a.expires_at).toLocaleString()}</span>}
      </div>
      <p>Why: {a.reason}</p>
      <p className="dim">rules: {a.rule_ids.join(", ")} · approvers: {a.approvers.join(", ") || "anyone with approvals:decide"}
        {a.summary && (a.summary as any).tainted && <span className="tag untrusted">untrusted text reached the model</span>}</p>
      <textarea value={edits} onChange={(e) => setEdits(e.target.value)} rows={5} />
      <input placeholder="comment" value={comment} onChange={(e) => setComment(e.target.value)} />
      <div className="buttons">
        <button className="approve" onClick={() => decide("approve", false)}>Approve</button>
        <button onClick={() => decide("approve", true)}>Approve with edits</button>
        <button className="reject" onClick={() => decide("reject", false)}>Reject</button>
      </div>
      {err && <p className="error">{err}</p>}
    </div>
  );
}

export default function Approvals() {
  const [status, setStatus] = useState("pending");
  const [list, setList] = useState<Approval[]>([]);
  const [err, setErr] = useState("");
  const load = () => api.approvals(status).then((r) => { setList(r.approvals); setErr(""); }).catch((e) => setErr(String(e)));
  useEffect(() => { load(); const t = setInterval(load, 3000); return () => clearInterval(t); },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [status]);
  return (
    <section>
      <div className="filters">
        {["pending", "approved", "rejected", "expired"].map((s) => (
          <button key={s} className={s === status ? "active" : ""} onClick={() => setStatus(s)}>{s}</button>
        ))}
      </div>
      {err && <p className="error">{err}</p>}
      {status === "pending"
        ? list.map((a) => <Card key={a.approval_id} a={a} onDone={load} />)
        : (
          <table className="runs"><tbody>
            {list.map((a) => (
              <tr key={a.approval_id}><td>{a.action.tool}</td><td><code>{JSON.stringify(a.action.args)}</code></td>
                <td>{a.status}</td><td>{a.decided_by}</td><td><Link to={`/runs/${a.run_id}`}>{short(a.run_id)}</Link></td></tr>
            ))}
          </tbody></table>
        )}
      {!list.length && <p className="dim">Nothing here.</p>}
    </section>
  );
}
