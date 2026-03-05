export function parseArgs(argv: string[]): Record<string, string | boolean> {
  const out: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    i += 1;
  }
  return out;
}

export function getStringArg(args: Record<string, string | boolean>, name: string, fallback?: string): string {
  const raw = args[name];
  if (typeof raw === "string") {
    return raw;
  }
  if (fallback !== undefined) {
    return fallback;
  }
  throw new Error(`Missing required argument --${name}`);
}
