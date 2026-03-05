import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { ConversationStore } from "../core/interfaces.js";
import { GitDbtRepositoryService } from "../adapters/dbt/dbtRepoService.js";

export interface InitTenantInput {
  appDataDir: string;
  tenantId: string;
  repoUrl: string;
  dbtSubpath: string;
  force?: boolean;
}

export interface InitTenantOutput {
  privateKeyPath: string;
  publicKeyPath: string;
  publicKey: string;
  localRepoPath: string;
}

export function initializeTenant(input: InitTenantInput, store: ConversationStore): InitTenantOutput {
  const keyDir = path.join(input.appDataDir, "keys", input.tenantId);
  const privateKeyPath = path.join(keyDir, "id_ed25519");
  const publicKeyPath = `${privateKeyPath}.pub`;
  const localRepoPath = GitDbtRepositoryService.buildLocalRepoPath(input.appDataDir, input.tenantId, input.repoUrl);

  fs.mkdirSync(keyDir, { recursive: true });
  fs.mkdirSync(path.dirname(localRepoPath), { recursive: true });

  if (!fs.existsSync(privateKeyPath) || input.force) {
    if (input.force) {
      try {
        fs.unlinkSync(privateKeyPath);
        fs.unlinkSync(publicKeyPath);
      } catch {
        // no-op
      }
    }

    execFileSync(
      "ssh-keygen",
      ["-t", "ed25519", "-N", "", "-C", `agent-blue-${input.tenantId}`, "-f", privateKeyPath],
      { stdio: "pipe" }
    );
    fs.chmodSync(privateKeyPath, 0o600);
  }

  const publicKey = fs.readFileSync(publicKeyPath, "utf8").trim();

  store.upsertTenantRepo({
    tenantId: input.tenantId,
    repoUrl: input.repoUrl,
    dbtSubpath: input.dbtSubpath,
    deployKeyPath: privateKeyPath,
    localPath: localRepoPath
  });

  return {
    privateKeyPath,
    publicKeyPath,
    publicKey,
    localRepoPath
  };
}
