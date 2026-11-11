import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, money, ms, Run, short } from "../api";

const STATUSES = ["", "pending", "running", "waiting", "completed", "failed", "cancelled"];

export default function Runs() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [filters, setFilters] = useState({ workflow: "", version: "", status: "", tenant: "" });
  const [minCost, setMinCost] = useState("");
  const [minDur, setMinDur] = useState("");
  const [err, setErr] = useState("");
  const [cursor, setCursor] = useState("");

  const load = (more = false) => {
    const q: Record<string, string> = { limit: "100" };
    Object.entries(filters).forEach(([k, v]) => v && (q[k] = v));
    if (more && cursor) q.cursor = cursor;
    api.runs(q).then((r) => { setRuns(more ? [...runs, ...r.runs] : r.runs); setCursor(r.next_cursor); setErr(""); })
      .catch((e) => setErr(String(e)));
  };
  useEffect(() => { load(); const t = setInterval(() => load(), 4000); return () => clearInterval(t); },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [filters]);

  const shown = runs.filter((r) => (!minCost || r.spent_usd >= Number(minCost)) &&
    (!minDur || (r.duration_ms ?? 0) >= Number(minDur) * 1000));
  const set = (k: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setFilters({ ...filters, [k]: e.target.value });

  return (
    <section>
      <div className="filters">
        <input placeholder="workflow" value={filters.workflow} onChange={set("workflow")} />
        <input placeholder="version" value={filters.version} onChange={set("version")} size={6} />
        <select value={filters.status} onChange={set("status")}>
          {STATUSES.map((s) => <option key={s} value={s}>{s || "any status"}</option>)}
        </select>
        <input placeholder="tenant (admin)" value={filters.tenant} onChange={set("tenant")} />
        <input placeholder="min cost $" value={minCost} onChange={(e) => setMinCost(e.target.value)} size={8} />
        <input placeholder="min duration s" value={minDur} onChange={(e) => setMinDur(e.target.value)} size={10} />
      </div>
      {err && <p className="error">{err}</p>}
      <table className="runs">
        <thead>
          <tr><th>run</th><th>workflow</th><th>status</th><th>tenant</th><th>cost</th><th>duration</th>
            <th>epoch</th><th>flags</th><th>created</th></tr>
        </thead>
        <tbody>
          {shown.map((r) => (
            <tr key={r.run_id}>
              <td><Link to={`/runs/${r.run_id}`} className="mono">{short(r.run_id)}</Link></td>
              <td>{r.workflow} <span className="dim">v{r.version}</span></td>
              <td><span className={`badge ${r.status}`}>{r.status}</span>{r.wait_reason && <span className="dim"> {r.wait_reason}</span>}</td>
              <td>{r.tenant_id}</td>
              <td>{money(r.spent_usd)}</td>
              <td>{ms(r.duration_ms)}</td>
              <td>{r.lease_epoch}</td>
              <td>
                {r.tainted && <span className="tag untrusted" title="untrusted text reached the model">tainted</span>}
                {r.dry_run && <span className="tag">dry run</span>}
                {r.forked_from && <span className="tag">fork</span>}
                {r.lease_epoch > 1 && <span className="tag" title="claimed more than once">resumed ×{r.lease_epoch - 1}</span>}
              </td>
              <td className="dim">{new Date(r.created_at).toLocaleTimeString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {cursor && <button onClick={() => load(true)}>Load more</button>}
    </section>
  );
}
