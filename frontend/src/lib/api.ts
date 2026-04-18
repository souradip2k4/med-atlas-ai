import axios from 'axios';

import { API_BASE_URL, MAPBOX_TOKEN } from './env';
import {
  bboxToPolygon,
  normalizeExtractMapMarkersResponse,
  normalizeFacilityProfile,
  normalizeFacilitySummary,
} from './format';
import type {
  BoundingBox,
  ExtractMapMarkersResponse,
  FacilityProfile,
  GeocodeResult,
  MapMetadata,
  MapSearchPayload,
  SearchResponse,
  AgentResponse,
} from './types';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export async function fetchMapMetadata(): Promise<MapMetadata> {
  const response = await api.get<MapMetadata>('/map/metadata');
  return response.data;
}

export async function searchFacilities(payload: MapSearchPayload): Promise<SearchResponse> {
  const response = await api.post<{ count: number; facilities: Record<string, unknown>[] }>(
    '/map/search',
    payload,
  );

  return {
    count: response.data.count,
    facilities: response.data.facilities.map(normalizeFacilitySummary),
  };
}

export async function fetchFacilityProfile(identifier: string): Promise<FacilityProfile> {
  const response = await api.get<Record<string, unknown>>(
    `/map/facility/${encodeURIComponent(identifier)}`,
  );

  if ('error' in response.data) {
    throw new Error(String(response.data.error));
  }

  return normalizeFacilityProfile(response.data);
}

export async function geocodePlace(query: string): Promise<GeocodeResult | null> {
  if (!MAPBOX_TOKEN || !query.trim()) {
    return null;
  }

  const encoded = encodeURIComponent(query);
  const response = await axios.get<{
    features?: Array<{
      place_name?: string;
      center?: [number, number];
      bbox?: [number, number, number, number];
    }>;
  }>(`https://api.mapbox.com/geocoding/v5/mapbox.places/${encoded}.json`, {
    params: {
      access_token: MAPBOX_TOKEN,
      country: 'gh',
      limit: 1,
      types: 'region,district,place,locality,neighborhood',
    },
  });

  const feature = response.data.features?.[0];
  if (!feature?.center) {
    return null;
  }

  const bbox = feature.bbox
    ? ([feature.bbox[1], feature.bbox[0], feature.bbox[3], feature.bbox[2]] as BoundingBox)
    : null;

  return {
    center: feature.center,
    bbox,
    placeName: feature.place_name ?? query,
  };
}

export function getHighlightFeature(result: GeocodeResult | null) {
  if (!result?.bbox) {
    return null;
  }

  return bboxToPolygon(result.bbox);
}

export async function invokeAgent(userMessage: string): Promise<AgentResponse> {
  const response = await api.post<AgentResponse>('/invoke', {
    messages: [{ role: 'user', content: userMessage }],
  });
  return response.data;
}

export async function extractMapMarkers(markdown: string): Promise<ExtractMapMarkersResponse> {
  const response = await api.post<Record<string, unknown>>('/map/extract-map-markers', {
    markdown,
  });

  return normalizeExtractMapMarkersResponse(response.data);
}
