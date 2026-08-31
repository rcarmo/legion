// src/index.ts
class LegionClient {
  baseUrl;
  apiKey;
  requestFetch;
  constructor(options) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.apiKey = options.apiKey;
    this.requestFetch = options.fetch ?? globalThis.fetch;
    if (!this.requestFetch)
      throw new Error("A Fetch API implementation is required");
  }
  async request(path, init = {}) {
    const headers = new Headers(init.headers);
    headers.set("accept", "application/json");
    if (init.body)
      headers.set("content-type", "application/json");
    if (this.apiKey)
      headers.set("authorization", `Bearer ${this.apiKey}`);
    const response = await this.requestFetch(`${this.baseUrl}${path}`, { ...init, headers });
    const value = await response.json();
    if (!response.ok)
      throw new Error(value.error ?? `Legion request failed: ${response.status}`);
    return value;
  }
  health() {
    return this.request("/health");
  }
  listSessions(query = "") {
    return this.request(`/sessions${query}`);
  }
  createSession(config) {
    return this.request("/sessions", { method: "POST", body: JSON.stringify(config) });
  }
  getSession(id) {
    return this.request(`/sessions/${encodeURIComponent(id)}`);
  }
  getLog(id) {
    return this.request(`/sessions/${encodeURIComponent(id)}/log`);
  }
  sendMessage(id, content) {
    return this.request(`/sessions/${encodeURIComponent(id)}/messages`, {
      method: "POST",
      body: JSON.stringify({ content })
    });
  }
  listAgents() {
    return this.request("/agents");
  }
  registerAgent(profile) {
    return this.request("/agents", { method: "POST", body: JSON.stringify(profile) });
  }
  invokeAgent(name, prompt, parent) {
    return this.request(`/agents/${encodeURIComponent(name)}/invoke`, {
      method: "POST",
      body: JSON.stringify({ prompt, parent_run_id: parent?.runId, at_seq: parent?.atSeq })
    });
  }
  runWorkflow(nodes) {
    return this.request("/workflows/run", {
      method: "POST",
      body: JSON.stringify({ nodes })
    });
  }
  listFunctions() {
    return this.request("/functions");
  }
  invokeFunction(name, args) {
    return this.request(`/functions/${encodeURIComponent(name)}/invoke`, {
      method: "POST",
      body: JSON.stringify(args)
    });
  }
}
export {
  LegionClient
};
