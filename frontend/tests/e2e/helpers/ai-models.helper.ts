import type { APIRequestContext } from '@playwright/test';

const MODELS_API = '/api/v1/configurationManager/ai-models';

type ModelType = 'llm' | 'embedding';
type ConfiguredModel = { modelKey: string; isDefault?: boolean };

/**
 * Where a real model comes from, mirroring the integration tests'
 * `helper/ai_models_setup.py`: OpenAI with TEST_OPENAI_API_KEY (the CI secret),
 * or, for local runs, any OpenAI-compatible server at E2E_AI_ENDPOINT.
 */
function modelConfig(type: ModelType): { provider: string; configuration: Record<string, string> } | null {
  const endpoint = process.env.E2E_AI_ENDPOINT;
  if (endpoint) {
    return {
      provider: 'openAICompatible',
      configuration: {
        endpoint,
        apiKey: process.env.E2E_AI_API_KEY || 'unused',
        model: (type === 'llm' ? process.env.E2E_AI_LLM_MODEL : process.env.E2E_AI_EMBEDDING_MODEL) || `e2e-${type}`,
      },
    };
  }
  const apiKey = process.env.TEST_OPENAI_API_KEY || process.env.OPENAI_API_KEY;
  if (!apiKey) return null;
  const model =
    type === 'llm'
      ? process.env.TEST_OPENAI_LLM_MODEL || 'gpt-5.4-nano'
      : process.env.TEST_OPENAI_EMBEDDING_MODEL || 'text-embedding-3-small';
  return { provider: 'openAI', configuration: { model, apiKey } };
}

async function listModels(api: APIRequestContext, type: ModelType): Promise<ConfiguredModel[]> {
  const res = await api.get(`${MODELS_API}/${type}`);
  if (!res.ok()) throw new Error(`listing ${type} models failed: ${res.status()} ${await res.text()}`);
  return ((await res.json()) as { models?: ConfiguredModel[] }).models ?? [];
}

/**
 * Make sure the org has a chat model and a cloud embedding model, which
 * indexing and answering both need. Models already configured are reused.
 *
 * Returns undefined when none are configured and no credentials say how to
 * add one, so the caller can skip with that reason. Otherwise returns a
 * function that removes whatever this call added; it takes the request
 * context to use then, since a hook's own context is gone by teardown.
 */
export async function ensureAnsweringModels(
  api: APIRequestContext,
): Promise<((cleanupApi: APIRequestContext) => Promise<void>) | undefined> {
  const added: Array<{ type: ModelType; modelKey: string }> = [];
  for (const type of ['llm', 'embedding'] as const) {
    if ((await listModels(api, type)).length > 0) continue;
    const config = modelConfig(type);
    if (!config) return undefined;
    // The backend health-checks the model before saving it.
    const res = await api.post(`${MODELS_API}/providers`, {
      data: {
        modelType: type,
        provider: config.provider,
        configuration: config.configuration,
        isMultimodal: false,
        isReasoning: false,
        isDefault: true,
        contextLength: null,
      },
      timeout: 120_000,
    });
    if (!res.ok()) throw new Error(`adding the test ${type} model failed: ${res.status()} ${await res.text()}`);
    const modelKey = ((await res.json()) as { details?: { modelKey?: string } }).details?.modelKey;
    if (!modelKey) throw new Error(`adding the test ${type} model returned no model key`);
    added.push({ type, modelKey });
  }
  return async (cleanupApi) => {
    for (const { type, modelKey } of added.reverse()) {
      const res = await cleanupApi.delete(`${MODELS_API}/providers/${type}/${modelKey}`);
      if (!res.ok() && res.status() !== 404) {
        throw new Error(`removing the test ${type} model failed: ${res.status()} ${await res.text()}`);
      }
    }
  };
}

export const NO_MODEL_REASON =
  'No AI model is configured and neither TEST_OPENAI_API_KEY nor E2E_AI_ENDPOINT is set; answering needs a real model';
