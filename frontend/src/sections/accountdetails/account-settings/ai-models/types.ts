
export type ModelType = 'llm' | 'embedding';
export type ProviderId = string;

export interface ConfiguredModel {
  id: string;
  modelKey?: string;
  name: string;
  provider: string;
  modelType: ModelType;
  configuration: Record<string, any>;
  isActive: boolean;
  isDefault: boolean;
  isMultimodal?: boolean;
  createdAt?: string;
  updatedAt?: string;
}

export interface ModelProvider {
  id: string;
  name: string;
  description: string;
  icon: string;
  supportedTypes: ModelType[];
  isPopular?: boolean;
  color: string;
}

export interface ModelData {
  provider: string;
  configuration: Record<string, any>;
  isMultimodal?: boolean;
  isDefault?: boolean;
  name?: string;
}

export const AVAILABLE_MODEL_PROVIDERS: ModelProvider[] = [
  {
    id: 'openAI',
    name: 'OpenAI',
    description: 'GPT models for text generation and embeddings',
    icon: 'simple-icons:openai',
    supportedTypes: ['llm', 'embedding'],
    isPopular: true,
    color: '#10A37F',
  },
  {
    id: 'anthropic',
    name: 'Anthropic',
    description: 'Claude models for advanced text processing',
    icon: 'simple-icons:anthropic',
    supportedTypes: ['llm'],
    isPopular: true,
    color: '#D97706',
  },
  {
    id: 'gemini',
    name: 'Gemini',
    description: 'Gemini models with multimodal capabilities',
    icon: 'logos:google-gemini',
    supportedTypes: ['llm', 'embedding'],
    isPopular: true,
    color: '#4285F4',
  },
  {
    id: 'azureOpenAI',
    name: 'Azure-OpenAI',
    description: 'Enterprise-grade OpenAI models',
    icon: 'vscode-icons:file-type-azure',
    supportedTypes: ['llm', 'embedding'],
    color: '#0078D4',
  },
  {
    id: 'cohere',
    name: 'Cohere',
    description: 'Command models for text generation and embeddings',
    icon: 'simple-icons:cohere',
    supportedTypes: ['llm', 'embedding'],
    color: '#39C5BB',
  },
  {
    id: 'ollama',
    name: 'Ollama',
    description: 'Local open-source models',
    icon: 'simple-icons:ollama',
    supportedTypes: ['llm', 'embedding'],
    color: '#4A90E2',
  },
  {
    id: 'groq',
    name: 'Groq',
    description: 'High-speed inference for LLM models',
    icon: 'simple-icons:groq',
    supportedTypes: ['llm'],
    color: '#F55036',
  },
  {
    id: 'xai',
    name: 'XAI',
    description: 'Grok models with real-time capabilities',
    icon: 'simple-icons:x',
    supportedTypes: ['llm'],
    color: '#1DA1F2',
  },
  {
    id: 'together',
    name: 'Together',
    description: 'Open-source models at scale',
    icon: 'mdi:account-group',
    supportedTypes: ['llm', 'embedding'],
    color: '#7C3AED',
  },
  {
    id: 'fireworks',
    name: 'Fireworks',
    description: 'Fast inference for generative AI',
    icon: 'mdi:fire',
    supportedTypes: ['llm'],
    color: '#FF6B35',
  },
  {
    id: 'mistral',
    name: 'Mistral',
    description: 'High-performance language models',
    icon: 'simple-icons:mistral',
    supportedTypes: ['llm'],
    color: '#FF7000',
  },
  {
    id: 'huggingface',
    name: 'HuggingFace',
    description: 'Open-source transformer models',
    icon: 'simple-icons:huggingface',
    supportedTypes: ['embedding'],
    color: '#FFD21E',
  },
];

export const MODEL_TYPE_CONFIGS = {
  llm: {
    name: 'Large Language Models',
    description: 'Text generation and comprehension models',
    icon: 'carbon:machine-learning-model',
    color: '#4CAF50',
  },
  embedding: {
    name: 'Embedding Models',
    description: 'Text vectorization for semantic search',
    icon: 'mdi:magnify',
    color: '#9C27B0',
  },
};

// Rest of the interfaces remain the same...
export interface ApiResponse {
  status: string;
  data?: any;
  models?: ConfiguredModel[];
  message?: string;
}

export interface ModelConfig {
  modelType: string;
  [key: string]: any;
}

export interface HealthCheckResult {
  success: boolean;
  message?: string;
}

export interface ModelTemplate {
  id: string;
  name: string;
  provider: string;
  configuration: Record<string, any>;
}

export interface ModelStatistics {
  totalModels: number;
  llmModels: number;
  embeddingModels: number;
  defaultModels: number;
}