import type { CSSProperties } from 'react';

import { compactText, formatLabel, getFacilityInitials, getFacilityTint } from '../lib/format';
import type { FacilitySummary, MapMetadata, SearchFilters } from '../lib/types';
import { CityIcon, LocationPinIcon, SlidersIcon } from './Icons';

interface ResultsSidebarProps {
  metadata: MapMetadata | undefined;
  filters: SearchFilters;
  facilities: FacilitySummary[];
  count: number;
  isLoading: boolean;
  isError: boolean;
  errorMessage: string;
  advancedOpen: boolean;
  selectedFacilityId: string | null;
  hoveredFacilityId: string | null;
  onAdvancedToggle: () => void;
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

  return (
    <button
      type="button"
      className={`result-card ${selected ? 'is-selected' : ''} ${hovered ? 'is-hovered' : ''}`}
      onClick={onSelect}
      onMouseEnter={() => onHover(facility.facility_id)}
      onMouseLeave={() => onHover(null)}
    >
      <div className="result-card__content">
        <span className="result-card__badge">{formatLabel(facility.facility_type) || 'Facility'}</span>
        <div className="result-card__eyebrow">
          {facility.organization_type ? formatLabel(facility.organization_type) : 'Healthcare network'}
        </div>
        <h3>{facility.facility_name}</h3>
        <p>{compactText(facility.description, 'Profile available. Open this facility to inspect specialties, contacts, and location details.')}</p>
        <div className="result-card__meta">
          <span>
            <LocationPinIcon />
            {[facility.city, facility.state].filter(Boolean).join(', ') || 'Location not listed'}
          </span>
          <span>
            <CityIcon />
            {facility.operator_type ? formatLabel(facility.operator_type) : 'Operator unknown'}
          </span>
        </div>
      </div>
      <div className="result-card__media" style={{ '--card-tint': tint } as CSSProperties}>
        <span>{getFacilityInitials(facility.facility_name)}</span>
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
  selectedFacilityId,
  hoveredFacilityId,
  onAdvancedToggle,
  onAdvancedFilterChange,
  onAffiliationToggle,
  onFacilitySelect,
  onFacilityHover,
  onClearSearch,
}: ResultsSidebarProps) {
  const hasSearch = Boolean(filters.region);

  return (
    <aside className="results-panel">
      <div className="results-panel__header">
        <div>
          <div className="results-panel__eyebrow">Med-Atlas AI</div>
          <h1>Results</h1>
        </div>
        <button
          type="button"
          className="clear-link"
          onClick={onClearSearch}
          disabled={!hasSearch}
        >
          Clear Search
        </button>
      </div>

      <div className="results-panel__summary">
        <div className="summary-chip">
          <span className="summary-chip__label">Selected region</span>
          <strong>{filters.region || 'None yet'}</strong>
        </div>
        <div className="summary-chip">
          <span className="summary-chip__label">Results found</span>
          <strong>{hasSearch ? count : '--'}</strong>
        </div>
      </div>

      <div className="advanced-panel">
        <button
          type="button"
          className={`advanced-panel__toggle ${advancedOpen ? 'is-open' : ''}`}
          onClick={onAdvancedToggle}
        >
          <span className="advanced-panel__copy">
            <SlidersIcon />
            More filters
          </span>
          <span className="advanced-panel__count">
            {[filters.facilityType, filters.operatorType, filters.organizationType, filters.affiliationTypes.length > 0 ? 'yes' : ''].filter(Boolean).length}
          </span>
        </button>

        {advancedOpen ? (
          <div className="advanced-panel__body">
            <label className="advanced-field">
              <span>Facility type</span>
              <select
                value={filters.facilityType}
                onChange={(event) =>
                  onAdvancedFilterChange('facilityType', event.target.value)
                }
              >
                <option value="">All facility types</option>
                {metadata?.facility_types.map((item) => (
                  <option key={item} value={item}>
                    {formatLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            <label className="advanced-field">
              <span>Operator type</span>
              <select
                value={filters.operatorType}
                onChange={(event) =>
                  onAdvancedFilterChange('operatorType', event.target.value)
                }
              >
                <option value="">All operators</option>
                {metadata?.operator_types.map((item) => (
                  <option key={item} value={item}>
                    {formatLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            <label className="advanced-field">
              <span>Organization</span>
              <select
                value={filters.organizationType}
                onChange={(event) =>
                  onAdvancedFilterChange('organizationType', event.target.value)
                }
              >
                <option value="">All organizations</option>
                {metadata?.organization_types.map((item) => (
                  <option key={item} value={item}>
                    {formatLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            <div className="advanced-field advanced-field--chips">
              <span>Affiliation types</span>
              <div className="chip-cloud">
                {metadata?.affiliation_types.map((item) => {
                  const selected = filters.affiliationTypes.includes(item);
                  return (
                    <button
                      type="button"
                      key={item}
                      className={`filter-chip ${selected ? 'is-selected' : ''}`}
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

      <div className="results-panel__body">
        {!hasSearch ? (
          <div className="results-placeholder">
            <strong>Choose a Ghana region to load facilities.</strong>
            <p>
              The map will zoom into the selected area, populate pins, and show the
              matching medical profiles here.
            </p>
          </div>
        ) : null}

        {hasSearch && isLoading ? (
          <div className="results-placeholder">
            <strong>Loading facilities</strong>
            <p>Pulling hospitals, clinics, and supporting profiles for the selected map area.</p>
          </div>
        ) : null}

        {hasSearch && isError ? (
          <div className="results-placeholder is-error">
            <strong>Search unavailable</strong>
            <p>{errorMessage}</p>
          </div>
        ) : null}

        {hasSearch && !isLoading && !isError && count === 0 ? (
          <div className="results-placeholder">
            <strong>No matching facilities</strong>
            <p>
              Try removing a city, specialty, or advanced filter. The selected region
              remains highlighted on the map.
            </p>
          </div>
        ) : null}

        {hasSearch && !isLoading && !isError && count > 0 ? (
          <div className="results-list">
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
