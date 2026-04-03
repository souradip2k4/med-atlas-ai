import {
  Building2,
  MapPin,
  SlidersHorizontal,
} from 'lucide-react';

import { compactText, countActiveAdvancedFilters, formatLabel } from '../lib/format';
import type { FacilitySummary, SearchFilters } from '../lib/types';

interface ResultsSidebarProps {
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

  const cardClass = selected || hovered
    ? 'grid grid-cols-1 items-start gap-4.5 rounded-panel border-0 bg-surface-card-strong p-4.5 text-left shadow-card-active transition duration-180'
    : 'grid grid-cols-1 items-start gap-4.5 rounded-panel border-0 bg-surface-card p-4.5 text-left shadow-card transition duration-180 hover:-translate-y-0.5 hover:bg-surface-card-strong hover:shadow-card-active';

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
        <h3 className="mb-2.5 text-lg font-semibold leading-[1.08] text-ink-900">{facility.facility_name}</h3>
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
    </button>
  );
}

function ResultsSkeletonCard() {
  return (
    <div className="grid grid-cols-1 gap-4.5 rounded-panel bg-surface-card p-4.5 shadow-card">
      <div className="skeleton-block h-8 w-24 rounded-full" />
      <div className="grid gap-2.5">
        <div className="skeleton-block h-4 w-20 rounded-full" />
        <div className="skeleton-block h-8 w-4/5 rounded-2xl" />
        <div className="grid gap-2">
          <div className="skeleton-block h-4 w-full rounded-full" />
          <div className="skeleton-block h-4 w-[92%] rounded-full" />
          <div className="skeleton-block h-4 w-2/3 rounded-full" />
        </div>
      </div>
      <div className="grid gap-2.5">
        <div className="skeleton-block h-4 w-1/2 rounded-full" />
        <div className="skeleton-block h-4 w-[42%] rounded-full" />
      </div>
    </div>
  );
}

export function ResultsSidebar({
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
  onFacilitySelect,
  onFacilityHover,
  onClearSearch,
}: ResultsSidebarProps) {
  const hasSearch = Boolean(filters.region);
  const advancedFilterCount = countActiveAdvancedFilters(filters);

  return (
    <aside
      className={`relative z-10 flex h-dvh flex-col overflow-y-auto border-r border-r-border-app bg-white/90 px-3.5 pb-6 pt-6 shadow-sidebar backdrop-blur-[14px] transition-[transform,opacity] duration-300 ease-out max-[920px]:absolute max-[920px]:inset-x-3 max-[920px]:bottom-3 max-[920px]:h-auto max-[920px]:min-h-0 max-[920px]:max-h-[44dvh] max-[920px]:rounded-panel max-[920px]:border max-[920px]:border-border-white-strong max-[920px]:px-2 max-[920px]:shadow-sidebar-mobile ${
        sidebarOpen ? 'translate-x-0 opacity-100' : '-translate-x-6 opacity-0'
      }`}
    >
      <div className="mb-4 font-bold flex items-center justify-between gap-4">
        <div className="items-center text-eyebrow uppercase tracking-[0.16em] text-accent-600">
          Med-Atlas AI
        </div>

        <button
          type="button"
          className="border-0 bg-transparent font-semibold text-accent-600 disabled:cursor-not-allowed disabled:text-ink-300"
          onClick={onClearSearch}
          disabled={!hasSearch}
        >
          Clear Search
        </button>
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
          className={
            advancedOpen
              ? 'flex w-full items-center justify-between rounded-[26px] border border-[rgba(79,141,247,0.24)] bg-[linear-gradient(180deg,rgba(239,246,255,0.98),rgba(232,241,255,0.96))] px-5 py-4 text-ink-900 shadow-[0_14px_28px_rgba(53,103,190,0.08)]'
              : 'flex w-full items-center justify-between rounded-[26px] border border-[rgba(232,238,247,0.96)] bg-[linear-gradient(180deg,rgba(244,247,252,0.98),rgba(239,244,250,0.98))] px-5 py-4 text-ink-900 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.95)]'
          }
          onClick={onAdvancedToggle}
        >
          <span className="inline-flex items-center gap-2.5 font-semibold">
            <span className="inline-flex size-10 items-center justify-center rounded-2xl bg-white text-accent-600 shadow-[0_8px_16px_rgba(69,119,191,0.08)]">
              <SlidersHorizontal className="size-[18px]" />
            </span>
            <span>
              <span className="block text-[1.06rem] text-ink-900">Healthcare filters</span>
              {/* <span className="block text-[0.82rem] font-normal text-ink-500">
                Narrow facilities by type, operator, and affiliation
              </span> */}
            </span>
          </span>
          <span className="inline-flex h-7 min-w-7 items-center justify-center rounded-full bg-surface-accent px-2 font-bold text-accent-700">
            {advancedFilterCount}
          </span>
        </button>
      </div>

      <div className="min-h-0 flex-1">
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
          <div className="grid gap-4">
            <div className="grid gap-4">
              <ResultsSkeletonCard />
              <ResultsSkeletonCard />
              <ResultsSkeletonCard />
            </div>
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
          <div className="grid gap-4 pr-1">
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
