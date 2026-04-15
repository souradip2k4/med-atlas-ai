export type DropdownKey = 'region' | 'city' | 'specialty' | null;
export type ThemeMode = 'light' | 'dark';
export type ThemePreference = ThemeMode | 'system';

export interface MapMetadata {
  regions: string[];
  cities_by_region: Record<string, string[]>;
  specialties: string[];
  affiliation_types: string[];
  facility_types: string[];
  operator_types: string[];
  organization_types: string[];
}

export interface SearchFilters {
  region: string;
  city: string;
  specialties: string[];
  facilityType: string;
  operatorType: string;
  organizationType: string;
  affiliationTypes: string[];
}

export interface FacilitySummary {
  facility_id: string;
  facility_name: string;
  latitude: number | null;
  longitude: number | null;
  city: string | null;
  state: string | null;
  year_established: number | null;
  facility_type: string | null;
  operator_type: string | null;
  organization_type: string | null;
  affiliation_types: string[] | null;
  description: string | null;
}

export interface FacilityProfile extends FacilitySummary {
  specialties: string[] | null;
  procedures: string[] | null;
  equipment: string[] | null;
  capabilities: string[] | null;
  address_line1: string | null;
  address_line2: string | null;
  address_line3: string | null;
  country: string | null;
  country_code: string | null;
  phone_numbers: string[] | null;
  email: string | null;
  websites: string[] | null;
  social_links: string[] | null;
  officialWebsite: string | null;
  accepts_volunteers: boolean | null;
  capacity: number | null;
  no_doctors: number | null;
  mission_statement: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface SearchResponse {
  count: number;
  facilities: FacilitySummary[];
}

export type BoundingBox = [number, number, number, number];

export interface MapSearchPayload {
  region: string;
  city?: string;
  specialties?: string[];
  facility_type?: string;
  operator_type?: string;
  organization_type?: string;
  affiliation_types?: string[];
  bbox?: BoundingBox;
}

export interface GeocodeResult {
  center: [number, number];
  bbox: BoundingBox | null;
  placeName: string;
}

// ── Agent Chat Types ──────────────────────────────────────────

export interface AgentOutputItem {
  type: string;
  content?: string;
  role?: string;
  tool_name?: string;
  call_id?: string;
  arguments?: string;
  output?: string;
}

export interface CitationSource {
  source_type: string;
  fact_id?: string;
  facility_id?: string;
  facility_name?: string;
  fact_type?: string;
  excerpt?: string;
  latitude?: number;
  longitude?: number;
}

export interface CitationStep {
  step_index: number;
  tool_name: string;
  call_id: string;
  query_used: string;
  tables_accessed: string[];
  sources: CitationSource[];
}

export interface CitationSummary {
  total_sources: number;
  facilities_referenced: string[];
  tools_used: string[];
  tables_accessed: string[];
}

export interface AgentCitations {
  steps: CitationStep[];
  summary: CitationSummary;
}

export interface AgentResponse {
  output: AgentOutputItem[];
  citations: AgentCitations;
  agent: string;
  endpoint: string;
}

export interface ChatEntry {
  id: string;
  userMessage: string;
  assistantMessage: string | null;
  citations: AgentCitations | null;
  isLoading: boolean;
  isError: boolean;
  errorMessage: string | null;
  referencedFacilities: Array<{
    facility_id: string;
    facility_name: string;
    latitude: number;
    longitude: number;
  }>;
}
