export type Run = {
  run_id: string;
  tenant_id: string;
  workflow: string;
  version: string;
  status: string;
  input: unknown;
  result?: unknown;
  error?: { type?: string; message?: string } | null;
  next_seq: number;
  lease_owner?: string;
  lease_epoch: number;
  wait_reason?: string;
  cancel_requested: boolean;
  spent_usd: number;
  budget_usd?: number;
  tainted: boolean;
  dry_run: boolean;
  forked_from?: string;
  parent_run_id?: string;
  continued_as?: string;
  created_at: string;
  finished_at?: string;
  duration_ms?: number;
};

export type KEvent = {
  run_id: string;
  seq: number;
  type: string;
  step_id?: string;
  payload: Record<string, any>;
  epoch: number;
  created_at: string;
};

export type Approval = {
  approval_id: string;
  run_id: string;
  step_id: string;
  action: { tool: string; args: Record<string, unknown>; risk?: string; schema?: unknown };
  reason: string;
  rule_ids: string[];
  approvers: string[];
  summary?: Record<string, unknown>;
  status: string;
  decided_by?: string;
  expires_at?: string;
  created_at: string;
};

const key = () => localStorage.getItem("keel.apiKey") || "";

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (key()) headers.Authorization = `Bearer ${key()}`;
  const r = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  const data = r.status === 204 ? null : await r.json().catch(() => null);
  if (!r.ok) throw new Error(data?.error?.message || `${r.status} ${r.statusText}`);
  return data as T;
}

export const api = {
  runs: (q: Record<string, string>) =>
    req<{ runs: Run[]; next_cursor: string }>("GET", "/v1/runs?" + new URLSearchParams(q).toString()),
  run: (id: string) => req<Run>("GET", `/v1/runs/${id}`),
  events: (id: string, reveal = false) =>
    req<{ events: KEvent[]; status: string }>("GET", `/v1/runs/${id}/events${reveal ? "?reveal_pii=true" : ""}`),
  fork: (id: string, from_seq: number, overrides: unknown, dry_run: boolean) =>
    req<{ run_id: string }>("POST", `/v1/runs/${id}/fork`, { from_seq, overrides, dry_run }),
  cancel: (id: string) => req("POST", `/v1/runs/${id}/cancel`),
  signal: (id: string, name: string, payload: unknown) => req("POST", `/v1/runs/${id}/signals/${name}`, { payload }),
  approvals: (status: string) => req<{ approvals: Approval[] }>("GET", `/v1/approvals?status=${status}`),
  decide: (id: string, decision: string, edited_args?: unknown, comment?: string) =>
    req("POST", `/v1/approvals/${id}`, { decision, edited_args, comment }),
  workflows: () => req<{ workflows: { name: string; versions: string[]; workers: number }[] }>("GET", "/v1/workflows"),
};

export function stream(runId: string, onEvent: (e: KEvent) => void, onToken: (t: { step_id: string; delta: string }) => void,
                       afterSeq: number): () => void {
  const params = new URLSearchParams({ after_seq: String(afterSeq) });
  if (key()) params.set("api_key", key());
  // EventSource sends Accept: text/event-stream, which selects the live stream.
  const es = new EventSource(`/v1/runs/${runId}/events?${params}`);
  es.addEventListener("event", (m) => onEvent(JSON.parse((m as MessageEvent).data)));
  es.addEventListener("token", (m) => onToken(JSON.parse((m as MessageEvent).data)));
  es.addEventListener("end", () => es.close());
  es.onerror = () => es.close();
  return () => es.close();
}

export const money = (v?: number) => (v === undefined || v === null ? "—" : `$${v.toFixed(v < 0.01 ? 5 : 3)}`);
export const ms = (v?: number) =>
  v === undefined ? "—" : v < 1000 ? `${Math.round(v)} ms` : v < 120000 ? `${(v / 1000).toFixed(1)} s` : `${(v / 60000).toFixed(1)} min`;
export const short = (id?: string) => (id ? id.slice(0, 8) : "");
