const rawEnv = import.meta.env as unknown as Record<string, string | undefined>;

export const MAPBOX_TOKEN =
  rawEnv.VITE_MAPBOX_TOKEN ?? '';

export const API_BASE_URL = rawEnv.VITE_API_BASE_URL ?? '';
