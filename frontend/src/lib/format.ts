import type {
  BoundingBox,
  ExtractedMapMarker,
  ExtractMapMarkersResponse,
  FacilityProfile,
  FacilitySummary,
  MapSearchPayload,
  SearchFilters,
} from './types';

export const GHANA_BOUNDS: [[number, number], [number, number]] = [
  [-3.45, 4.45],
  [1.75, 11.3],
];

export const GHANA_VIEW = {
  center: [-1.23, 7.95] as [number, number],
  zoom: 5.75,
};

const WORD_SPLIT = /[_-\s]+/g;

export function formatLabel(value: string | null | undefined): string {
  if (!value) {
    return '';
  }

  return value
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(WORD_SPLIT, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function parseNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

export function parseStringArray(value: unknown): string[] | null {
  if (!Array.isArray(value)) {
    return null;
  }

  const items = value
    .map((item) => (typeof item === 'string' ? item.trim() : ''))
    .filter(Boolean);

  return items.length > 0 ? items : null;
}

export function normalizeFacilitySummary(value: Record<string, unknown>): FacilitySummary {
  return {
    facility_id: String(value.facility_id ?? ''),
    facility_name: String(value.facility_name ?? 'Unnamed Facility'),
    latitude: parseNumber(value.latitude),
    longitude: parseNumber(value.longitude),
    city: typeof value.city === 'string' ? value.city : null,
    state: typeof value.state === 'string' ? value.state : null,
    year_established: parseNumber(value.year_established),
    facility_type: typeof value.facility_type === 'string' ? value.facility_type : null,
    operator_type: typeof value.operator_type === 'string' ? value.operator_type : null,
    organization_type:
      typeof value.organization_type === 'string' ? value.organization_type : null,
    affiliation_types: parseStringArray(value.affiliation_types),
    description: typeof value.description === 'string' ? value.description : null,
  };
}

export function normalizeFacilityProfile(value: Record<string, unknown>): FacilityProfile {
  const summary = normalizeFacilitySummary(value);

  return {
    ...summary,
    specialties: parseStringArray(value.specialties),
    procedures: parseStringArray(value.procedures),
    equipment: parseStringArray(value.equipment),
    capabilities: parseStringArray(value.capabilities),
    address_line1: typeof value.address_line1 === 'string' ? value.address_line1 : null,
    address_line2: typeof value.address_line2 === 'string' ? value.address_line2 : null,
    address_line3: typeof value.address_line3 === 'string' ? value.address_line3 : null,
    country: typeof value.country === 'string' ? value.country : null,
    country_code: typeof value.country_code === 'string' ? value.country_code : null,
    phone_numbers: parseStringArray(value.phone_numbers),
    email: typeof value.email === 'string' ? value.email : null,
    websites: parseStringArray(value.websites),
    social_links: parseStringArray(value.social_links),
    officialWebsite:
      typeof value.officialWebsite === 'string' ? value.officialWebsite : null,
    accepts_volunteers:
      typeof value.accepts_volunteers === 'boolean' ? value.accepts_volunteers : null,
    capacity: parseNumber(value.capacity),
    no_doctors: parseNumber(value.no_doctors),
    mission_statement:
      typeof value.mission_statement === 'string' ? value.mission_statement : null,
    created_at: typeof value.created_at === 'string' ? value.created_at : null,
    updated_at: typeof value.updated_at === 'string' ? value.updated_at : null,
  };
}

export function normalizeExtractedMapMarker(value: Record<string, unknown>): ExtractedMapMarker | null {
  const id = typeof value.id === 'string' ? value.id : '';
  const name = typeof value.name === 'string' ? value.name.trim() : '';
  const latitude = parseNumber(value.latitude);
  const longitude = parseNumber(value.longitude);

  if (!id || !name || latitude === null || longitude === null) {
    return null;
  }

  return {
    id,
    name,
    latitude,
    longitude,
  };
}

export function normalizeExtractMapMarkersResponse(
  value: Record<string, unknown>,
): ExtractMapMarkersResponse {
  const rawMarkers = Array.isArray(value.map_markers) ? value.map_markers : [];
  const seen = new Set<string>();
  const map_markers = rawMarkers
    .map((item) =>
      item && typeof item === 'object'
        ? normalizeExtractedMapMarker(item as Record<string, unknown>)
        : null,
    )
    .filter((item): item is ExtractedMapMarker => Boolean(item))
    .filter((item) => {
      const key = `${item.name}|${item.latitude}|${item.longitude}`;
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });

  return {
    map_markers,
    extracted_names: Array.isArray(value.extracted_names)
      ? value.extracted_names.filter((item): item is string => typeof item === 'string')
      : [],
    raw_sql_results: Array.isArray(value.raw_sql_results)
      ? value.raw_sql_results.filter(
          (item): item is Record<string, unknown> => Boolean(item && typeof item === 'object'),
        )
      : [],
  };
}

export function buildSearchPayload(
  filters: SearchFilters,
  bbox: BoundingBox | null,
): MapSearchPayload {
  const payload: MapSearchPayload = {
    region: filters.region,
  };

  if (filters.city) {
    payload.city = filters.city;
  }
  if (filters.specialties.length > 0) {
    payload.specialties = filters.specialties;
  }
  if (filters.facilityType) {
    payload.facility_type = filters.facilityType;
  }
  if (filters.operatorType) {
    payload.operator_type = filters.operatorType;
  }
  if (filters.organizationType) {
    payload.organization_type = filters.organizationType;
  }
  if (filters.affiliationTypes.length > 0) {
    payload.affiliation_types = filters.affiliationTypes;
  }
  if (bbox) {
    payload.bbox = bbox;
  }

  return payload;
}

export function countActiveAdvancedFilters(filters: SearchFilters): number {
  return [
    filters.facilityType,
    filters.operatorType,
    filters.organizationType,
    filters.affiliationTypes.length > 0 ? 'affiliations' : '',
  ].filter(Boolean).length;
}

export function getFacilityInitials(name: string): string {
  const words = name
    .split(/\s+/)
    .map((item) => item.trim())
    .filter(Boolean)
    .slice(0, 2);

  return words.map((word) => word[0]?.toUpperCase() ?? '').join('') || 'MF';
}

export function getFacilityTint(type: string | null | undefined): string {
  switch (type) {
    case 'hospital':
      return 'var(--tone-blue)';
    case 'clinic':
      return 'var(--tone-teal)';
    case 'doctor':
      return 'var(--tone-gold)';
    case 'dentist':
      return 'var(--tone-lilac)';
    default:
      return 'var(--tone-ink-soft)';
  }
}

export function compactText(value: string | null | undefined, fallback: string): string {
  if (!value || !value.trim()) {
    return fallback;
  }

  return value.trim();
}

export function getAddressLines(profile: FacilityProfile): string[] {
  return [profile.address_line1, profile.address_line2, profile.address_line3]
    .filter((item): item is string => Boolean(item && item.trim()));
}

export function bboxToPolygon(bbox: BoundingBox) {
  const [minLat, minLon, maxLat, maxLon] = bbox;

  return {
    type: 'Feature' as const,
    geometry: {
      type: 'Polygon' as const,
      coordinates: [[
        [minLon, minLat],
        [maxLon, minLat],
        [maxLon, maxLat],
        [minLon, maxLat],
        [minLon, minLat],
      ]],
    },
    properties: {},
  };
}
