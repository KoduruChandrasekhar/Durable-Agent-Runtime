import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { api, KEvent, money } from "../api";

type Row = { step: string; a?: KEvent; b?: KEvent };

function summary(e?: KEvent): string {
  if (!e) return "—";
  const o = e.payload.output;
  if (e.payload.kind === "llm") return (o?.tool_calls?.length ? `tools: ${o.tool_calls.map((t: any) => t.name).join(", ")} ` : "") + (o?.text ?? "");
  return JSON.stringify(o);
}

export default function Diff() {
  const [params, setParams] = useSearchParams();
  const [a, setA] = useState(params.get("a") || "");
  const [b, setB] = useState(params.get("b") || "");
  const [rows, setRows] = useState<Row[]>([]);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (!params.get("a") || !params.get("b")) return;
    Promise.all([api.events(params.get("a")!), api.events(params.get("b")!)]).then(([ea, eb]) => {
      const done = (evs: KEvent[]) => new Map(evs.filter((e) => e.type === "StepCompleted").map((e) => [e.step_id!, e]));
      const ma = done(ea.events), mb = done(eb.events);
      const order = [...new Set([...ma.keys(), ...mb.keys()])];
      setRows(order.map((s) => ({ step: s, a: ma.get(s), b: mb.get(s) })));
      setErr("");
    }).catch((e) => setErr(String(e)));
  }, [params]);

  return (
    <section>
      <div className="filters">
        <input placeholder="run A" value={a} onChange={(e) => setA(e.target.value)} size={38} />
        <input placeholder="run B" value={b} onChange={(e) => setB(e.target.value)} size={38} />
        <button onClick={() => setParams({ a, b })}>Compare</button>
      </div>
      {err && <p className="error">{err}</p>}
      <table className="diff">
        <thead><tr><th>step</th><th>run A</th><th>run B</th></tr></thead>
        <tbody>
          {rows.map((r) => {
            const sa = summary(r.a), sb = summary(r.b);
            return (
              <tr key={r.step} className={sa === sb ? "" : "changed"}>
                <td className="mono">{r.step}</td>
                <td><pre>{sa}</pre><span className="dim">{r.a ? money(r.a.payload.cost_usd) : ""}</span></td>
                <td><pre>{sb}</pre><span className="dim">{r.b ? money(r.b.payload.cost_usd) : ""}</span></td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </section>
  );
}
