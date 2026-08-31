export interface LegionClientOptions {
  baseUrl: string;
  apiKey?: string;
  fetch?: typeof globalThis.fetch;
}

export interface SessionCreate {
  model: string;
  system_prompt?: string;
  budget?: Record<string, number>;
}

export interface AgentProfile {
  name: string;
  description?: string;
  config: SessionCreate & { tools?: string[]; metadata?: unknown };
}

export interface WorkflowNode {
  id: string;
  tool: string;
  args?: Record<string, unknown>;
  depends_on?: string[];
}

/** Zero-dependency Legion REST client for Bun and Node.js 20+. */
export class LegionClient {
  readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly requestFetch: typeof globalThis.fetch;

  constructor(options: LegionClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.apiKey = options.apiKey;
    this.requestFetch = options.fetch ?? globalThis.fetch;
    if (!this.requestFetch) throw new Error("A Fetch API implementation is required");
  }

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set("accept", "application/json");
    if (init.body) headers.set("content-type", "application/json");
    if (this.apiKey) headers.set("authorization", `Bearer ${this.apiKey}`);
    const response = await this.requestFetch(`${this.baseUrl}${path}`, { ...init, headers });
    const value = await response.json() as T & { error?: string };
    if (!response.ok) throw new Error(value.error ?? `Legion request failed: ${response.status}`);
    return value;
  }

  health() { return this.request<{ ok: boolean; version: string }>("/health"); }
  listSessions(query = "") { return this.request<{ sessions: unknown[] }>(`/sessions${query}`); }
  createSession(config: SessionCreate) {
    return this.request<{ id: string; status: string }>("/sessions", { method: "POST", body: JSON.stringify(config) });
  }
  getSession(id: string) { return this.request(`/sessions/${encodeURIComponent(id)}`); }
  getLog(id: string) { return this.request(`/sessions/${encodeURIComponent(id)}/log`); }
  sendMessage(id: string, content: string) {
    return this.request<{ response: string }>(`/sessions/${encodeURIComponent(id)}/messages`, {
      method: "POST", body: JSON.stringify({ content }),
    });
  }
  listAgents() { return this.request<{ agents: AgentProfile[] }>("/agents"); }
  registerAgent(profile: AgentProfile) {
    return this.request("/agents", { method: "POST", body: JSON.stringify(profile) });
  }
  invokeAgent(name: string, prompt: string, parent?: { runId: string; atSeq: number }) {
    return this.request(`/agents/${encodeURIComponent(name)}/invoke`, {
      method: "POST",
      body: JSON.stringify({ prompt, parent_run_id: parent?.runId, at_seq: parent?.atSeq }),
    });
  }
  runWorkflow(nodes: WorkflowNode[]) {
    return this.request<{ outputs: Record<string, unknown>; waves: string[][] }>("/workflows/run", {
      method: "POST", body: JSON.stringify({ nodes }),
    });
  }
  listFunctions() { return this.request<{ functions: unknown[] }>("/functions"); }
  invokeFunction(name: string, args: unknown) {
    return this.request(`/functions/${encodeURIComponent(name)}/invoke`, {
      method: "POST", body: JSON.stringify(args),
    });
  }
}
