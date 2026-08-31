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
    config: SessionCreate & {
        tools?: string[];
        metadata?: unknown;
    };
}
export interface WorkflowNode {
    id: string;
    tool: string;
    args?: Record<string, unknown>;
    depends_on?: string[];
}
/** Zero-dependency Legion REST client for Bun and Node.js 20+. */
export declare class LegionClient {
    readonly baseUrl: string;
    private readonly apiKey?;
    private readonly requestFetch;
    constructor(options: LegionClientOptions);
    private request;
    health(): Promise<{
        ok: boolean;
        version: string;
    }>;
    listSessions(query?: string): Promise<{
        sessions: unknown[];
    }>;
    createSession(config: SessionCreate): Promise<{
        id: string;
        status: string;
    }>;
    getSession(id: string): Promise<unknown>;
    getLog(id: string): Promise<unknown>;
    sendMessage(id: string, content: string): Promise<{
        response: string;
    }>;
    listAgents(): Promise<{
        agents: AgentProfile[];
    }>;
    registerAgent(profile: AgentProfile): Promise<unknown>;
    invokeAgent(name: string, prompt: string, parent?: {
        runId: string;
        atSeq: number;
    }): Promise<unknown>;
    runWorkflow(nodes: WorkflowNode[]): Promise<{
        outputs: Record<string, unknown>;
        waves: string[][];
    }>;
    listFunctions(): Promise<{
        functions: unknown[];
    }>;
    invokeFunction(name: string, args: unknown): Promise<unknown>;
}
