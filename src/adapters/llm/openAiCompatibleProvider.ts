import { LlmMessage, LlmProvider } from "../../core/interfaces.js";

interface ChatCompletionResponse {
  choices?: Array<{
    message?: {
      content?: string;
    };
  }>;
}

export class OpenAiCompatibleProvider implements LlmProvider {
  constructor(
    private readonly baseUrl: string,
    private readonly apiKey: string,
    private readonly extraHeaders: Record<string, string> = {}
  ) {}

  async generateText(input: { model: string; messages: LlmMessage[]; temperature?: number }): Promise<string> {
    if (!this.apiKey) {
      throw new Error("LLM_API_KEY is not configured.");
    }

    const response = await fetch(`${this.baseUrl.replace(/\/$/, "")}/chat/completions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.apiKey}`,
        ...this.extraHeaders
      },
      body: JSON.stringify({
        model: input.model,
        messages: input.messages,
        temperature: input.temperature ?? 0.2
      })
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(`LLM request failed (${response.status}): ${body}`);
    }

    const data = (await response.json()) as ChatCompletionResponse;
    const text = data.choices?.[0]?.message?.content?.trim();
    if (!text) {
      throw new Error("LLM returned empty response.");
    }
    return text;
  }
}
