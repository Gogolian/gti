/**
 * Pi provider extension for a gti LLM proxy.
 *
 * Copy this file to ~/.pi/agent/extensions/gti.ts (or .pi/extensions/gti.ts in
 * a project) and start `gti llm-proxy`. Pi will discover the model catalog from
 * the proxy's /v1/models endpoint instead of requiring a static models.json
 * entry.
 *
 * Environment:
 *   GTI_BASE_URL        default http://127.0.0.1:8765/v1
 *   GTI_CONTEXT_WINDOW  fallback when the server does not report context size
 *   GTI_MAX_TOKENS      output limit advertised to Pi
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const DEFAULT_BASE_URL = "http://127.0.0.1:8765/v1";
const DEFAULT_CONTEXT_WINDOW = 32768;
const DEFAULT_MAX_TOKENS = 4096;

function envNumber(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

interface ModelEntry {
  id?: unknown;
  context_length?: unknown;
  max_tokens?: unknown;
  meta?: { n_ctx?: unknown };
}

export default function (pi: ExtensionAPI) {
  const baseUrl = (process.env.GTI_BASE_URL ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
  const fallbackContext = envNumber("GTI_CONTEXT_WINDOW", DEFAULT_CONTEXT_WINDOW);
  const maxTokens = envNumber("GTI_MAX_TOKENS", DEFAULT_MAX_TOKENS);

  pi.registerProvider("gti", {
    name: "GTI",
    baseUrl,
    apiKey: "gti",
    api: "openai-completions",
    async refreshModels({ signal }) {
      const response = await fetch(`${baseUrl}/models`, { signal });
      if (!response.ok) {
        throw new Error(`gti /v1/models returned HTTP ${response.status}`);
      }
      const payload = (await response.json()) as { data?: ModelEntry[] };
      if (!Array.isArray(payload.data)) {
        throw new Error("gti /v1/models did not return a data array");
      }
      return payload.data
        .filter((model) => typeof model.id === "string" && model.id.length > 0)
        .map((model) => {
          const reported = model.context_length ?? model.meta?.n_ctx;
          const contextWindow =
            typeof reported === "number" && reported > 0 ? reported : fallbackContext;
          return {
            id: model.id as string,
            name: model.id as string,
            reasoning: false,
            input: ["text" as const],
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
            contextWindow,
            maxTokens,
          };
        });
    },
  });
}
