export type Role = "system" | "user" | "assistant" | "tool";

export interface ConversationMessage {
  id: string;
  tenantId: string;
  conversationId: string;
  role: Role;
  content: string;
  createdAt: string;
}

export interface AgentProfile {
  id: string;
  tenantId: string;
  name: string;
  soulPrompt: string;
  maxRowsPerQuery: number;
  allowedDbtPathPrefixes: string[];
  createdAt: string;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
}

export interface DbtModelInfo {
  name: string;
  relativePath: string;
}

export interface AgentContext {
  tenantId: string;
  profileName: string;
  conversationId: string;
  llmModel?: string;
}

export interface AgentResponse {
  text: string;
  debug?: Record<string, unknown>;
}
