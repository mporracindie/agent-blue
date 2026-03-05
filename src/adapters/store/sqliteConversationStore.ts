import Database from "better-sqlite3";
import path from "node:path";
import fs from "node:fs";
import { ConversationStore } from "../../core/interfaces.js";
import { AgentContext, AgentProfile, ConversationMessage } from "../../core/types.js";
import { createId } from "../../utils/id.js";

const DEFAULT_SOUL_PROMPT = [
  "You are an analytical assistant for business stakeholders.",
  "Be precise, avoid hallucinations, and communicate assumptions.",
  "Prefer concise summaries with clear numbers and caveats."
].join(" ");

export class SqliteConversationStore implements ConversationStore {
  private readonly db: Database.Database;

  constructor(dbPath: string) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
  }

  init(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS conversations (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        profile_name TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS agent_profiles (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        name TEXT NOT NULL,
        soul_prompt TEXT NOT NULL,
        max_rows_per_query INTEGER NOT NULL,
        allowed_dbt_path_prefixes TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(tenant_id, name)
      );

      CREATE TABLE IF NOT EXISTS tenant_repos (
        tenant_id TEXT PRIMARY KEY,
        repo_url TEXT NOT NULL,
        dbt_subpath TEXT NOT NULL,
        deploy_key_path TEXT NOT NULL,
        local_path TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `);
  }

  createConversation(context: AgentContext): void {
    const existing = this.db
      .prepare("SELECT id FROM conversations WHERE id = ?")
      .get(context.conversationId) as { id: string } | undefined;
    if (existing) {
      return;
    }

    this.db
      .prepare(
        "INSERT INTO conversations (id, tenant_id, profile_name, created_at) VALUES (?, ?, ?, datetime('now'))"
      )
      .run(context.conversationId, context.tenantId, context.profileName);
  }

  addMessage(message: Omit<ConversationMessage, "id" | "createdAt">): ConversationMessage {
    const id = createId("msg");
    const createdAt = new Date().toISOString();
    this.db
      .prepare(
        "INSERT INTO messages (id, tenant_id, conversation_id, role, content, created_at) VALUES (?, ?, ?, ?, ?, ?)"
      )
      .run(id, message.tenantId, message.conversationId, message.role, message.content, createdAt);
    return { ...message, id, createdAt };
  }

  getMessages(conversationId: string, limit = 20): ConversationMessage[] {
    const rows = this.db
      .prepare(
        `SELECT id, tenant_id, conversation_id, role, content, created_at
         FROM messages
         WHERE conversation_id = ?
         ORDER BY created_at DESC
         LIMIT ?`
      )
      .all(conversationId, limit) as Array<{
      id: string;
      tenant_id: string;
      conversation_id: string;
      role: ConversationMessage["role"];
      content: string;
      created_at: string;
    }>;

    return rows
      .reverse()
      .map((r) => ({
        id: r.id,
        tenantId: r.tenant_id,
        conversationId: r.conversation_id,
        role: r.role,
        content: r.content,
        createdAt: r.created_at
      }));
  }

  getOrCreateProfile(tenantId: string, profileName: string): AgentProfile {
    const row = this.db
      .prepare(
        `SELECT id, tenant_id, name, soul_prompt, max_rows_per_query, allowed_dbt_path_prefixes, created_at
         FROM agent_profiles
         WHERE tenant_id = ? AND name = ?`
      )
      .get(tenantId, profileName) as
      | {
          id: string;
          tenant_id: string;
          name: string;
          soul_prompt: string;
          max_rows_per_query: number;
          allowed_dbt_path_prefixes: string;
          created_at: string;
        }
      | undefined;

    if (row) {
      return {
        id: row.id,
        tenantId: row.tenant_id,
        name: row.name,
        soulPrompt: row.soul_prompt,
        maxRowsPerQuery: row.max_rows_per_query,
        allowedDbtPathPrefixes: JSON.parse(row.allowed_dbt_path_prefixes),
        createdAt: row.created_at
      };
    }

    const id = createId("profile");
    const createdAt = new Date().toISOString();
    const prefixes = ["models"];

    this.db
      .prepare(
        `INSERT INTO agent_profiles
         (id, tenant_id, name, soul_prompt, max_rows_per_query, allowed_dbt_path_prefixes, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      )
      .run(id, tenantId, profileName, DEFAULT_SOUL_PROMPT, 200, JSON.stringify(prefixes), createdAt);

    return {
      id,
      tenantId,
      name: profileName,
      soulPrompt: DEFAULT_SOUL_PROMPT,
      maxRowsPerQuery: 200,
      allowedDbtPathPrefixes: prefixes,
      createdAt
    };
  }

  upsertTenantRepo(input: {
    tenantId: string;
    repoUrl: string;
    dbtSubpath: string;
    deployKeyPath: string;
    localPath: string;
  }): void {
    this.db
      .prepare(
        `INSERT INTO tenant_repos (tenant_id, repo_url, dbt_subpath, deploy_key_path, local_path, updated_at)
         VALUES (?, ?, ?, ?, ?, datetime('now'))
         ON CONFLICT(tenant_id) DO UPDATE SET
           repo_url = excluded.repo_url,
           dbt_subpath = excluded.dbt_subpath,
           deploy_key_path = excluded.deploy_key_path,
           local_path = excluded.local_path,
           updated_at = excluded.updated_at`
      )
      .run(input.tenantId, input.repoUrl, input.dbtSubpath, input.deployKeyPath, input.localPath);
  }

  getTenantRepo(tenantId: string): {
    tenantId: string;
    repoUrl: string;
    dbtSubpath: string;
    deployKeyPath: string;
    localPath: string;
  } | null {
    const row = this.db
      .prepare(
        "SELECT tenant_id, repo_url, dbt_subpath, deploy_key_path, local_path FROM tenant_repos WHERE tenant_id = ?"
      )
      .get(tenantId) as
      | {
          tenant_id: string;
          repo_url: string;
          dbt_subpath: string;
          deploy_key_path: string;
          local_path: string;
        }
      | undefined;
    if (!row) {
      return null;
    }
    return {
      tenantId: row.tenant_id,
      repoUrl: row.repo_url,
      dbtSubpath: row.dbt_subpath,
      deployKeyPath: row.deploy_key_path,
      localPath: row.local_path
    };
  }
}
