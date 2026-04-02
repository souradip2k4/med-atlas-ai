import type { CSSProperties } from 'react';
import {
  Building2,
  MapPin,
  PanelLeftClose,
  SlidersHorizontal,
} from 'lucide-react';

import { compactText, formatLabel, getFacilityInitials, getFacilityTint } from '../lib/format';
import type { FacilitySummary, MapMetadata, SearchFilters } from '../lib/types';

interface ResultsSidebarProps {
  metadata: MapMetadata | undefined;
  filters: SearchFilters;
  facilities: FacilitySummary[];
  count: number;
  isLoading: boolean;
  isError: boolean;
  errorMessage: string;
  advancedOpen: boolean;
  sidebarOpen: boolean;
  selectedFacilityId: string | null;
  hoveredFacilityId: string | null;
  onAdvancedToggle: () => void;
  onSidebarToggle: () => void;
  onAdvancedFilterChange: (
    key: 'facilityType' | 'operatorType' | 'organizationType',
    value: string,
  ) => void;
  onAffiliationToggle: (value: string) => void;
  onFacilitySelect: (facilityId: string) => void;
  onFacilityHover: (facilityId: string | null) => void;
  onClearSearch: () => void;
}

function ResultCard({
  facility,
  selected,
  hovered,
  onSelect,
  onHover,
}: {
  facility: FacilitySummary;
  selected: boolean;
  hovered: boolean;
  onSelect: () => void;
  onHover: (facilityId: string | null) => void;
}) {
  const tint = getFacilityTint(facility.facility_type);
  const cardClass = selected || hovered
    ? 'grid grid-cols-[minmax(0,1fr)_106px] items-start gap-4.5 rounded-panel border-0 bg-surface-card-strong p-4.5 text-left shadow-card-active transition duration-180'
    : 'grid grid-cols-[minmax(0,1fr)_106px] items-start gap-4.5 rounded-panel border-0 bg-surface-card p-4.5 text-left shadow-card transition duration-180 hover:-translate-y-0.5 hover:bg-surface-card-strong hover:shadow-card-active';

  return (
    <button
      type="button"
      className={cardClass}
      onClick={onSelect}
      onMouseEnter={() => onHover(facility.facility_id)}
      onMouseLeave={() => onHover(null)}
    >
      <div>
        <span className="mb-2.5 inline-flex rounded-full bg-surface-teal px-3 py-1.5 text-eyebrow font-bold uppercase tracking-[0.08em] text-tone-teal">
          {formatLabel(facility.facility_type) || 'Facility'}
        </span>
        <div className="mb-2 text-ui-sm text-ink-500">
          {facility.organization_type ? formatLabel(facility.organization_type) : 'Healthcare network'}
        </div>
        <h3 className="mb-2.5 text-[1.35rem] leading-[1.08] text-ink-900">{facility.facility_name}</h3>
        <p className="mb-4 text-ui text-ink-600">
          {compactText(
            facility.description,
            'Profile available. Open this facility to inspect specialties, contacts, and location details.',
          )}
        </p>
        <div className="grid gap-2 text-ui-sm text-ink-500">
          <span className="inline-flex items-center gap-2">
            <MapPin className="size-4 text-accent-600" />
            {[facility.city, facility.state].filter(Boolean).join(', ') || 'Location not listed'}
          </span>
          <span className="inline-flex items-center gap-2">
            <Building2 className="size-4 text-accent-600" />
            {facility.operator_type ? formatLabel(facility.operator_type) : 'Operator unknown'}
          </span>
        </div>
      </div>
      <div
        className="relative grid min-h-[120px] place-items-end overflow-hidden rounded-card bg-[linear-gradient(145deg,color-mix(in_srgb,var(--card-tint)_68%,white_32%),rgba(255,255,255,0.3)),linear-gradient(180deg,rgba(255,255,255,0.7),rgba(255,255,255,0))] p-3.5 before:absolute before:inset-2.5 before:rounded-chip before:bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.65),transparent_34%),linear-gradient(135deg,rgba(255,255,255,0.16),rgba(255,255,255,0))] before:content-['']"
        style={{ '--card-tint': tint } as CSSProperties}
      >
        <span className="relative z-[1] inline-flex size-[54px] items-center justify-center rounded-chip bg-white/88 text-[1.2rem] font-extrabold text-ink-900 shadow-avatar">
          {getFacilityInitials(facility.facility_name)}
        </span>
      </div>
    </button>
  );
}

export function ResultsSidebar({
  metadata,
  filters,
  facilities,
  count,
  isLoading,
  isError,
  errorMessage,
  advancedOpen,
  sidebarOpen,
  selectedFacilityId,
  hoveredFacilityId,
  onAdvancedToggle,
  onSidebarToggle,
  onAdvancedFilterChange,
  onAffiliationToggle,
  onFacilitySelect,
  onFacilityHover,
  onClearSearch,
}: ResultsSidebarProps) {
  const hasSearch = Boolean(filters.region);

  return (
    <aside
      className={`relative z-10 flex min-h-dvh flex-col border-r border-r-border-app bg-white/90 px-6 pb-6 pt-7 shadow-sidebar backdrop-blur-[14px] transition duration-300 max-[920px]:absolute max-[920px]:inset-x-3 max-[920px]:bottom-3 max-[920px]:min-h-0 max-[920px]:max-h-[44dvh] max-[920px]:overflow-hidden max-[920px]:rounded-panel max-[920px]:border max-[920px]:border-border-white-strong max-[920px]:shadow-sidebar-mobile ${
        sidebarOpen ? 'translate-x-0 opacity-100' : '-translate-x-8 opacity-0'
      }`}
    >
      <div className="mb-4.5 flex items-start justify-between gap-4">
        <div>
          <div className="mb-2 text-eyebrow uppercase tracking-[0.16em] text-accent-600">
            Med-Atlas AI
          </div>
          <h1 className="text-[clamp(2rem,3.2vw,2.55rem)] leading-[0.96] text-ink-900">Results</h1>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            className="inline-flex size-10 items-center justify-center rounded-full border border-white/90 bg-white/92 text-accent-600 shadow-chip transition hover:bg-white"
            onClick={onSidebarToggle}
            aria-label="Hide results sidebar"
          >
            <PanelLeftClose className="size-[18px]" />
          </button>
          <button
            type="button"
            className="border-0 bg-transparent font-semibold text-accent-600 disabled:cursor-not-allowed disabled:text-ink-300"
            onClick={onClearSearch}
            disabled={!hasSearch}
          >
            Clear Search
          </button>
        </div>
      </div>

      <div className="mb-4.5 grid grid-cols-2 gap-3">
        <div className="rounded-card bg-surface-panel-soft px-4 py-3.5 shadow-inset-accent">
          <span className="mb-1.5 block text-eyebrow text-ink-500">Selected region</span>
          <strong className="block text-base text-ink-900">{filters.region || 'None yet'}</strong>
        </div>
        <div className="rounded-card bg-surface-panel-soft px-4 py-3.5 shadow-inset-accent">
          <span className="mb-1.5 block text-eyebrow text-ink-500">Results found</span>
          <strong className="block text-base text-ink-900">{hasSearch ? count : '--'}</strong>
        </div>
      </div>

      <div className="mb-5">
        <button
          type="button"
          className="flex w-full items-center justify-between rounded-card border-0 bg-surface-filter px-4.5 py-3.5 text-ink-900"
          onClick={onAdvancedToggle}
        >
          <span className="inline-flex items-center gap-2.5 font-semibold">
            <SlidersHorizontal className="size-[18px] text-accent-600" />
            More filters
          </span>
          <span className="inline-flex h-7 min-w-7 items-center justify-center rounded-full bg-surface-accent px-2 font-bold text-accent-700">
            {[filters.facilityType, filters.operatorType, filters.organizationType, filters.affiliationTypes.length > 0 ? 'yes' : ''].filter(Boolean).length}
          </span>
        </button>

        {advancedOpen ? (
          <div className="mt-3.5 grid gap-3.5 rounded-pill bg-white/82 p-4 shadow-[inset_0_0_0_1px_var(--color-border-panel)]">
            <label className="grid gap-2 text-ui-sm text-ink-600">
              <span>Facility type</span>
              <select
                value={filters.facilityType}
                onChange={(event) =>
                  onAdvancedFilterChange('facilityType', event.target.value)
                }
                className="min-h-12 rounded-2xl border border-border-field bg-white px-3.5 text-ink-900"
              >
                <option value="">All facility types</option>
                {metadata?.facility_types.map((item) => (
                  <option key={item} value={item}>
                    {formatLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            <label className="grid gap-2 text-ui-sm text-ink-600">
              <span>Operator type</span>
              <select
                value={filters.operatorType}
                onChange={(event) =>
                  onAdvancedFilterChange('operatorType', event.target.value)
                }
                className="min-h-12 rounded-2xl border border-border-field bg-white px-3.5 text-ink-900"
              >
                <option value="">All operators</option>
                {metadata?.operator_types.map((item) => (
                  <option key={item} value={item}>
                    {formatLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            <label className="grid gap-2 text-ui-sm text-ink-600">
              <span>Organization</span>
              <select
                value={filters.organizationType}
                onChange={(event) =>
                  onAdvancedFilterChange('organizationType', event.target.value)
                }
                className="min-h-12 rounded-2xl border border-border-field bg-white px-3.5 text-ink-900"
              >
                <option value="">All organizations</option>
                {metadata?.organization_types.map((item) => (
                  <option key={item} value={item}>
                    {formatLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            <div className="grid items-start gap-2 text-ui-sm text-ink-600">
              <span>Affiliation types</span>
              <div className="flex flex-wrap gap-2">
                {metadata?.affiliation_types.map((item) => {
                  const selected = filters.affiliationTypes.includes(item);
                  return (
                    <button
                      type="button"
                      key={item}
                      className={
                        selected
                          ? 'rounded-full border border-border-highlight-soft bg-surface-accent px-3 py-2 text-accent-700'
                          : 'rounded-full border border-border-field bg-surface-panel-strong px-3 py-2 text-ink-700'
                      }
                      onClick={() => onAffiliationToggle(item)}
                    >
                      {formatLabel(item)}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        ) : null}
      </div>

      <div className="min-h-0 flex-1 overflow-hidden">
        {!hasSearch ? (
          <div className="grid gap-2 rounded-panel bg-surface-panel p-6 text-ink-600 shadow-inset-soft">
            <strong className="text-ink-900">Choose a Ghana region to load facilities.</strong>
            <p>
              The map will zoom into the selected area, populate pins, and show the
              matching medical profiles here.
            </p>
          </div>
        ) : null}

        {hasSearch && isLoading ? (
          <div className="grid gap-2 rounded-panel bg-surface-panel p-6 text-ink-600 shadow-inset-soft">
            <strong className="text-ink-900">Loading facilities</strong>
            <p>Pulling hospitals, clinics, and supporting profiles for the selected map area.</p>
          </div>
        ) : null}

        {hasSearch && isError ? (
          <div className="grid gap-2 rounded-panel bg-surface-error p-6 text-ink-600 shadow-inset-soft">
            <strong className="text-ink-900">Search unavailable</strong>
            <p>{errorMessage}</p>
          </div>
        ) : null}

        {hasSearch && !isLoading && !isError && count === 0 ? (
          <div className="grid gap-2 rounded-panel bg-surface-panel p-6 text-ink-600 shadow-inset-soft">
            <strong className="text-ink-900">No matching facilities</strong>
            <p>
              Try removing a city, specialty, or advanced filter. The selected region
              remains highlighted on the map.
            </p>
          </div>
        ) : null}

        {hasSearch && !isLoading && !isError && count > 0 ? (
          <div className="flex h-full flex-col gap-4 overflow-auto pr-1">
            {facilities.map((facility) => (
              <ResultCard
                key={facility.facility_id}
                facility={facility}
                selected={selectedFacilityId === facility.facility_id}
                hovered={hoveredFacilityId === facility.facility_id}
                onSelect={() => onFacilitySelect(facility.facility_id)}
                onHover={onFacilityHover}
              />
            ))}
          </div>
        ) : null}
      </div>
    </aside>
  );
}
