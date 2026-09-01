import { LegionClient } from "../../packages/client/dist/index.js";

export const baseUrl = (process.env.LEGION_URL ?? "http://127.0.0.1:18080").replace(/\/$/, "");
export const apiKey = process.env.LEGION_API_KEY;
export const legion = new LegionClient({ baseUrl, apiKey });

export async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("accept", "application/json");
  if (init.body) headers.set("content-type", "application/json");
  if (apiKey) headers.set("authorization", `Bearer ${apiKey}`);
  const response = await fetch(`${baseUrl}${path}`, { ...init, headers });
  const value = await response.json() as T & { error?: string };
  if (!response.ok) throw new Error(value.error ?? `${response.status} ${response.statusText}`);
  return value;
}

export function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

export function model(): string {
  return process.env.LEGION_MODEL ?? "anthropic/claude-haiku-3-5";
}
