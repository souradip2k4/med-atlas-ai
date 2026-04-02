import { useEffect, useRef } from 'react';
import type { ReactNode } from 'react';

import { countActiveAdvancedFilters, formatLabel } from '../lib/format';
import type { DropdownKey, MapMetadata, SearchFilters } from '../lib/types';
import {
  CityIcon,
  CloseIcon,
  RegionIcon,
  SearchIcon,
  SpecialtyIcon,
} from './Icons';

interface TopSearchBarProps {
  metadata: MapMetadata | undefined;
  filters: SearchFilters;
  activeDropdown: DropdownKey;
  onDropdownChange: (dropdown: DropdownKey) => void;
  onRegionSelect: (region: string) => void;
  onCitySelect: (city: string) => void;
  onToggleSpecialty: (specialty: string) => void;
  onClearRegion: () => void;
  onClearCity: () => void;
  onClearSpecialties: () => void;
}

interface FieldButtonProps {
  icon: ReactNode;
  label: string;
  value: string;
  active: boolean;
  disabled?: boolean;
  onClick: () => void;
  onClear?: () => void;
}

function FieldButton({
  icon,
  label,
  value,
  active,
  disabled,
  onClick,
  onClear,
}: FieldButtonProps) {
  return (
    <button
      type="button"
      className={`filter-pill ${active ? 'is-active' : ''}`}
      onClick={onClick}
      disabled={disabled}
    >
      <span className="filter-pill__icon">{icon}</span>
      <span className="filter-pill__body">
        <span className="filter-pill__label">{label}</span>
        <span className={`filter-pill__value ${value ? '' : 'is-placeholder'}`}>
          {value || `Add ${label.toLowerCase()}`}
        </span>
      </span>
      {onClear && value ? (
        <span
          className="filter-pill__clear"
          onClick={(event) => {
            event.stopPropagation();
            onClear();
          }}
          role="button"
          tabIndex={0}
          onKeyDown={(event) => {
            if (event.key === 'Enter' || event.key === ' ') {
              event.preventDefault();
              onClear();
            }
          }}
        >
          <CloseIcon />
        </span>
      ) : null}
    </button>
  );
}

export function TopSearchBar({
  metadata,
  filters,
  activeDropdown,
  onDropdownChange,
  onRegionSelect,
  onCitySelect,
  onToggleSpecialty,
  onClearRegion,
  onClearCity,
  onClearSpecialties,
}: TopSearchBarProps) {
  const shellRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!shellRef.current?.contains(event.target as Node)) {
        onDropdownChange(null);
      }
    }

    document.addEventListener('mousedown', handlePointerDown);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
    };
  }, [onDropdownChange]);

  const cityOptions = metadata?.cities_by_region[filters.region] ?? [];
  const specialtyCount = filters.specialties.length;
  const advancedCount = countActiveAdvancedFilters(filters);

  return (
    <div className="top-search-shell" ref={shellRef}>
      <div className="top-search">
        <FieldButton
          icon={<RegionIcon />}
          label="Region"
          value={filters.region}
          active={activeDropdown === 'region'}
          onClick={() =>
            onDropdownChange(activeDropdown === 'region' ? null : 'region')
          }
          onClear={filters.region ? onClearRegion : undefined}
        />
        <FieldButton
          icon={<CityIcon />}
          label="City"
          value={filters.city}
          active={activeDropdown === 'city'}
          disabled={!filters.region}
          onClick={() => onDropdownChange(activeDropdown === 'city' ? null : 'city')}
          onClear={filters.city ? onClearCity : undefined}
        />
        <FieldButton
          icon={<SpecialtyIcon />}
          label="Specialty"
          value={specialtyCount > 0 ? `${specialtyCount} specialties` : ''}
          active={activeDropdown === 'specialty'}
          onClick={() =>
            onDropdownChange(activeDropdown === 'specialty' ? null : 'specialty')
          }
          onClear={specialtyCount > 0 ? onClearSpecialties : undefined}
        />
        <div className="top-search__status">
          <span className="top-search__status-copy">
            {filters.region ? 'Ghana healthcare map' : 'Select a region to begin'}
          </span>
          <span className="top-search__status-accent">
            {advancedCount > 0 ? `${advancedCount} advanced` : 'Map search'}
          </span>
          <span className="top-search__status-icon">
            <SearchIcon />
          </span>
        </div>
      </div>

      {activeDropdown === 'region' ? (
        <div className="top-search-panel">
          <div className="top-search-panel__header">
            <span>Regions</span>
            <span>{metadata?.regions.length ?? 0}</span>
          </div>
          <div className="option-list">
            {metadata?.regions.map((region) => (
              <button
                type="button"
                key={region}
                className={`option-row ${filters.region === region ? 'is-selected' : ''}`}
                onClick={() => {
                  onRegionSelect(region);
                  onDropdownChange(null);
                }}
              >
                <RegionIcon className="option-row__icon" />
                <span>{region}</span>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {activeDropdown === 'city' ? (
        <div className="top-search-panel top-search-panel--compact">
          <div className="top-search-panel__header">
            <span>Cities in {filters.region || 'selected region'}</span>
            <span>{cityOptions.length}</span>
          </div>
          <div className="option-list">
            {cityOptions.length > 0 ? (
              cityOptions.map((city) => (
                <button
                  type="button"
                  key={city}
                  className={`option-row ${filters.city === city ? 'is-selected' : ''}`}
                  onClick={() => {
                    onCitySelect(city);
                    onDropdownChange(null);
                  }}
                >
                  <CityIcon className="option-row__icon" />
                  <span>{city}</span>
                </button>
              ))
            ) : (
              <div className="option-empty">Choose a region before narrowing to a city.</div>
            )}
          </div>
        </div>
      ) : null}

      {activeDropdown === 'specialty' ? (
        <div className="top-search-panel top-search-panel--wide">
          <div className="top-search-panel__header">
            <span>Specialties</span>
            <span>{metadata?.specialties.length ?? 0}</span>
          </div>
          <div className="option-list option-list--grid">
            {metadata?.specialties.map((specialty) => {
              const selected = filters.specialties.includes(specialty);
              return (
                <button
                  type="button"
                  key={specialty}
                  className={`option-row ${selected ? 'is-selected' : ''}`}
                  onClick={() => onToggleSpecialty(specialty)}
                >
                  <SpecialtyIcon className="option-row__icon" />
                  <span>{formatLabel(specialty)}</span>
                </button>
              );
            })}
          </div>
          {filters.specialties.length > 0 ? (
            <div className="selected-chip-row">
              {filters.specialties.map((specialty) => (
                <button
                  type="button"
                  key={specialty}
                  className="selected-chip"
                  onClick={() => onToggleSpecialty(specialty)}
                >
                  {formatLabel(specialty)}
                  <CloseIcon className="selected-chip__icon" />
                </button>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
