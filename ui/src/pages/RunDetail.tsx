import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api, KEvent, money, ms, Run, short, stream } from "../api";

type Step = {
  id: string;
  kind: string;
  start?: KEvent;
  end?: KEvent;
  failures: KEvent[];
  attempts: number;
  policy: KEvent[];
  approvals: KEvent[];
};

const SPECIAL = new Set(["RunStarted", "RunResumed", "RunCompleted", "RunFailed", "RunCancelled", "RunForked",
  "TimerStarted", "TimerFired", "SignalReceived", "ChildRunStarted", "ChildRunCompleted", "RunTainted",
  "CompensationStarted", "CompensationCompleted", "RunParked", "RunContinuedAsNew"]);

function groupSteps(events: KEvent[]): Step[] {
  const steps = new Map<string, Step>();
  for (const e of events) {
    if (!e.step_id || SPECIAL.has(e.type)) continue;
    const s = steps.get(e.step_id) ?? { id: e.step_id, kind: "", failures: [], attempts: 0, policy: [], approvals: [] };
    if (e.type === "StepStarted") { s.start = s.start ?? e; s.kind = e.payload.kind; s.attempts++; }
    if (e.type === "StepCompleted") s.end = e;
    if (e.type === "StepFailed") s.failures.push(e);
    if (e.type === "PolicyDecided") s.policy.push(e);
    if (e.type === "ApprovalRequested" || e.type === "ApprovalResolved") s.approvals.push(e);
    steps.set(e.step_id, s);
  }
  return [...steps.values()].filter((s) => s.start || s.policy.length);
}

function Trust({ t }: { t: string }) {
  return <span className={`tag ${t}`}>{t}</span>;
}

function Inspector({ step, events }: { step: Step; events: KEvent[] }) {
  const inp = step.start?.payload.input;
  const out = step.end?.payload.output;
  return (
    <div className="inspector">
      <h3 className="mono">{step.id} <span className="dim">{step.kind}</span></h3>
      {step.kind === "llm" && inp && (
        <>
          <p>model <b>{inp.model}</b>{step.end?.payload.model && step.end.payload.model !== inp.model &&
            <> → served by <b>{step.end.payload.model}</b></>} · tools: {(inp.tools || []).join(", ") || "none"}
            {inp.quarantined && <span className="tag">quarantined</span>}
            {step.start?.payload.tainted && <span className="tag untrusted">tainted call</span>}</p>
          <div className="messages">
            {(inp.messages || []).map((m: any, i: number) => (
              <div key={i} className={`msg ${m.role}`}>
                <div className="msghead">{m.role} <Trust t={m.trust} />{m.source && <span className="dim"> {m.source}</span>}</div>
                <pre>{m.content ?? JSON.stringify(m.tool_calls, null, 2)}</pre>
              </div>
            ))}
          </div>
          {out && (
            <div className="msg assistant">
              <div className="msghead">response · {out.usage?.input_tokens} in / {out.usage?.output_tokens} out ·
                {" "}{money(step.end?.payload.cost_usd)} · cache {out.cache_hit || "miss"}</div>
              <pre>{out.text ?? ""}{out.tool_calls?.length ? "\n" + JSON.stringify(out.tool_calls, null, 2) : ""}</pre>
            </div>
          )}
        </>
      )}
      {step.kind !== "llm" && (
        <>
          {inp?.idempotency_key && <p>idempotency key <code>{inp.idempotency_key}</code> · effect {inp.effect} · risk {inp.risk}</p>}
          <h4>input</h4><pre>{JSON.stringify(inp, null, 2)}</pre>
          <h4>output{step.end?.payload.reconciled && <span className="tag">reconciled</span>}</h4>
          <pre>{JSON.stringify(out, null, 2)}</pre>
        </>
      )}
      {step.policy.map((p) => (
        <p key={p.seq}>policy: <span className={`badge ${p.payload.decision}`}>{p.payload.decision}</span>{" "}
          {(p.payload.rule_ids || []).join(", ")} <span className="dim">{p.payload.reason}</span></p>
      ))}
      {step.approvals.map((a) => (
        <p key={a.seq}>{a.type === "ApprovalRequested" ? "approval requested" : `approval ${a.payload.decision} by ${a.payload.decided_by}`}
          {a.payload.edited_args && <> with edits <code>{JSON.stringify(a.payload.edited_args)}</code></>}</p>
      ))}
      {step.failures.map((f) => (
        <p key={f.seq} className="error">attempt {f.payload.attempt}: {f.payload.error?.type}: {f.payload.error?.message}
          {f.payload.retryable ? " (retrying)" : " (final)"}</p>
      ))}
      <p className="dim">{events.length} events in this run</p>
    </div>
  );
}

export default function RunDetail() {
  const { id = "" } = useParams();
  const nav = useNavigate();
  const [run, setRun] = useState<Run>();
  const [events, setEvents] = useState<KEvent[]>([]);
  const [tokens, setTokens] = useState("");
  const [sel, setSel] = useState<string>();
  const [reveal, setReveal] = useState(false);
  const [forkModel, setForkModel] = useState("mock-large");
  const [err, setErr] = useState("");

  useEffect(() => {
    let close = () => {};
    api.run(id).then(setRun).catch((e) => setErr(String(e)));
    api.events(id, reveal).then((r) => {
      setEvents(r.events);
      const last = r.events.length ? r.events[r.events.length - 1].seq : 0;
      if (!["completed", "failed", "cancelled"].includes(r.status)) {
        close = stream(id, (e) => { setEvents((p) => (p.some((x) => x.seq === e.seq) ? p : [...p, e])); api.run(id).then(setRun); },
          (t) => setTokens((s) => (s + t.delta).slice(-4000)), last);
      }
    }).catch((e) => setErr(String(e)));
    return () => close();
  }, [id, reveal]);

  const steps = useMemo(() => groupSteps(events), [events]);
  const t0 = events.length ? Date.parse(events[0].created_at) : 0;
  const tEnd = events.length ? Math.max(...events.map((e) => Date.parse(e.created_at))) : 1;
  const span = Math.max(1, tEnd - t0);
  const markers = events.filter((e) => ["RunResumed", "TimerFired", "SignalReceived", "ApprovalResolved",
    "RunTainted", "RunForked", "RunParked", "CompensationStarted"].includes(e.type));
  const selected = steps.find((s) => s.id === sel);

  const fork = async (seq: number) => {
    try {
      const r = await api.fork(id, seq, { model: forkModel }, true);
      nav(`/runs/${r.run_id}`);
    } catch (e) { setErr(String(e)); }
  };

  if (!run) return <p>{err || "loading…"}</p>;
  return (
    <section>
      <div className="runhead">
        <h2 className="mono">{short(run.run_id)} <span className="dim">{run.workflow} v{run.version}</span></h2>
        <span className={`badge ${run.status}`}>{run.status}</span>
        {run.wait_reason && <span className="dim">waiting on {run.wait_reason}</span>}
        <span>cost {money(run.spent_usd)}{run.budget_usd ? ` / ${money(run.budget_usd)}` : ""}</span>
        <span>duration {ms(run.duration_ms)}</span>
        <span>epoch {run.lease_epoch}</span>
        {run.tainted && <span className="tag untrusted">tainted</span>}
        {run.dry_run && <span className="tag">dry run</span>}
        {run.forked_from && <Link to={`/runs/${run.forked_from}`}>forked from {short(run.forked_from)}</Link>}
        {run.parent_run_id && <Link to={`/runs/${run.parent_run_id}`}>parent {short(run.parent_run_id)}</Link>}
        <label><input type="checkbox" checked={reveal} onChange={(e) => setReveal(e.target.checked)} /> reveal PII</label>
        {!["completed", "failed", "cancelled"].includes(run.status) &&
          <button onClick={() => api.cancel(id).then(() => api.run(id).then(setRun))}>Cancel</button>}
        <Link to={`/diff?a=${run.run_id}`}>Diff…</Link>
      </div>
      {err && <p className="error">{err}</p>}
      {run.error && <p className="error">{run.error.type}: {run.error.message}</p>}
      {run.result !== undefined && run.result !== null && <details><summary>result</summary><pre>{JSON.stringify(run.result, null, 2)}</pre></details>}

      <div className="layout">
        <div className="timeline">
          <div className="markers">
            {markers.map((m) => (
              <div key={m.seq} className={`marker ${m.type}`} style={{ left: `${((Date.parse(m.created_at) - t0) / span) * 100}%` }}
                title={`${m.type} ${m.payload.reason || m.payload.worker_id || ""}`} />
            ))}
          </div>
          {steps.map((s) => {
            const a = s.start ? Date.parse(s.start.created_at) - t0 : 0;
            const b = (s.end ? Date.parse(s.end.created_at) : s.failures.length ? Date.parse(s.failures[s.failures.length - 1].created_at) : tEnd) - t0;
            return (
              <div key={s.id} className={`row ${sel === s.id ? "sel" : ""}`} onClick={() => setSel(s.id)}>
                <div className="label mono">{s.id}</div>
                <div className="lane">
                  <div className={`bar ${s.kind} ${s.end ? "" : s.failures.length ? "failed" : "open"}`}
                    style={{ left: `${(a / span) * 100}%`, width: `${Math.max(0.6, ((b - a) / span) * 100)}%` }} />
                </div>
                <div className="meta">
                  {s.attempts > 1 && <span className="tag">×{s.attempts}</span>}
                  {s.end?.payload.cost_usd ? money(s.end.payload.cost_usd) : ""}
                  {s.end && <span className="dim"> {ms(s.end.payload.duration_ms)}</span>}
                  {s.end?.payload.reexecuted_after_crash && <span className="tag untrusted" title="re-run after a worker crash">re-run</span>}
                </div>
              </div>
            );
          })}
          <h4>events</h4>
          <table className="events">
            <tbody>
              {events.map((e) => (
                <tr key={e.seq} className={e.type === "RunResumed" ? "resumed" : ""}>
                  <td className="mono">{e.seq}</td>
                  <td className="dim">{new Date(e.created_at).toLocaleTimeString()}</td>
                  <td>e{e.epoch}</td>
                  <td>{e.type}</td>
                  <td className="mono" onClick={() => e.step_id && setSel(e.step_id)}>{e.step_id}</td>
                  <td className="dim small">{e.type === "RunResumed" ? `worker ${e.payload.worker_id} · ${e.payload.reason}` :
                    JSON.stringify(e.payload).slice(0, 90)}</td>
                  <td><button className="small" title="fork from here (dry run)" onClick={() => fork(e.seq)}>fork</button></td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="dim">Fork creates a dry-run copy of the journal up to that event and continues with model
            <input value={forkModel} onChange={(e) => setForkModel(e.target.value)} size={12} /></p>
        </div>
        <div className="side">
          {selected ? <Inspector step={selected} events={events} /> : <p className="dim">Select a step to inspect its prompt, response, tool call and policy decisions.</p>}
          {tokens && <div className="tokens"><h4>live tokens</h4><pre>{tokens}</pre></div>}
        </div>
      </div>
    </section>
  );
}
