import { useEffect, useRef } from 'react';
import type { ReactNode } from 'react';
import { MapPinned, MapPin, Stethoscope, X } from 'lucide-react';

import { formatLabel } from '../lib/format';
import type { DropdownKey, MapMetadata, SearchFilters } from '../lib/types';

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
  const buttonClass = active
    ? 'flex h-14 items-center gap-3 rounded-pill border-0 bg-surface-filter-strong px-3.5 py-2 text-left text-ink-900 shadow-inset-filter transition duration-200'
    : 'flex h-14 items-center gap-3 rounded-pill border-0 bg-transparent px-3.5 py-2 text-left text-ink-900 transition duration-200 hover:bg-surface-filter-strong hover:shadow-inset-filter';

  return (
    <button
      type="button"
      className={`${buttonClass} disabled:cursor-not-allowed disabled:opacity-55`}
      onClick={onClick}
      disabled={disabled}
    >
      <span className="inline-flex size-9 shrink-0 items-center justify-center rounded-full bg-surface-accent text-accent-600">
        {icon}
      </span>
      <span className="flex min-w-0 flex-1 flex-col">
        <span className="text-pill-label text-ink-500">{label}</span>
        <span
          className={`truncate text-base font-semibold ${
            value ? 'text-ink-900' : 'text-ink-400'
          }`}
        >
          {value || `Add ${label.toLowerCase()}`}
        </span>
      </span>
      {onClear && value ? (
        <span
          className="inline-flex size-6.5 items-center justify-center rounded-full bg-white/95 text-accent-600 shadow-chip"
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
          <X className="size-3.5" />
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

  return (
    <div
      className="absolute left-1/2 top-7 z-30 w-[min(880px,calc(100%-112px))] -translate-x-1/2 max-[920px]:top-3 max-[920px]:w-[calc(100%-24px)]"
      ref={shellRef}
    >
      <div className="grid animate-chrome-in grid-cols-3 gap-2 rounded-[30px] border border-white/88 bg-white/94 shadow-search backdrop-blur-[18px] max-w-[700px]">
        <FieldButton
          icon={<MapPinned className="size-5" />}
          label="Region"
          value={filters.region}
          active={activeDropdown === 'region'}
          onClick={() =>
            onDropdownChange(activeDropdown === 'region' ? null : 'region')
          }
          onClear={filters.region ? onClearRegion : undefined}
        />
        <FieldButton
          icon={<MapPin className="size-5" />}
          label="City"
          value={filters.city}
          active={activeDropdown === 'city'}
          disabled={!filters.region}
          onClick={() => onDropdownChange(activeDropdown === 'city' ? null : 'city')}
          onClear={filters.city ? onClearCity : undefined}
        />
        <FieldButton
          icon={<Stethoscope className="size-5" />}
          label="Specialty"
          value={specialtyCount > 0 ? `${specialtyCount} specialties` : ''}
          active={activeDropdown === 'specialty'}
          onClick={() =>
            onDropdownChange(activeDropdown === 'specialty' ? null : 'specialty')
          }
          onClear={specialtyCount > 0 ? onClearSpecialties : undefined}
        />
      </div>

      {activeDropdown === 'region' ? (
        <div className="mx-auto mt-3 w-[min(760px,100%)] animate-panel-in rounded-panel border border-white/95 bg-surface-panel-strong p-4.5 shadow-panel backdrop-blur-[16px]">
          <div className="mb-3.5 flex items-center justify-between text-panel-header text-ink-500">
            <span>Regions</span>
            <span>{metadata?.regions.length ?? 0}</span>
          </div>
          <div className="flex max-h-[320px] flex-col gap-2 overflow-auto">
            {metadata?.regions.map((region) => (
              <button
                type="button"
                key={region}
                className={`flex items-center gap-3 rounded-chip border px-4 py-3.5 text-left transition duration-150 ${
                  filters.region === region
                    ? 'border-border-highlight bg-surface-accent-strong'
                    : 'border-border-option bg-white hover:-translate-y-px hover:border-border-highlight hover:bg-surface-accent-strong'
                }`}
                onClick={() => {
                  onRegionSelect(region);
                  onDropdownChange(null);
                }}
              >
                <MapPinned className="size-[18px] shrink-0 text-accent-600" />
                <span>{region}</span>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {activeDropdown === 'city' ? (
        <div className="mx-auto mt-3 w-[min(560px,100%)] animate-panel-in rounded-panel border border-white/95 bg-surface-panel-strong p-4.5 shadow-panel backdrop-blur-[16px]">
          <div className="mb-3.5 flex items-center justify-between text-panel-header text-ink-500">
            <span>Cities in {filters.region || 'selected region'}</span>
            <span>{cityOptions.length}</span>
          </div>
          <div className="flex max-h-[320px] flex-col gap-2 overflow-auto">
            {cityOptions.length > 0 ? (
              cityOptions.map((city) => (
                <button
                  type="button"
                  key={city}
                  className={`flex items-center gap-3 rounded-chip border px-4 py-3.5 text-left transition duration-150 ${
                    filters.city === city
                      ? 'border-border-highlight bg-surface-accent-strong'
                      : 'border-border-option bg-white hover:-translate-y-px hover:border-border-highlight hover:bg-surface-accent-strong'
                  }`}
                  onClick={() => {
                    onCitySelect(city);
                    onDropdownChange(null);
                  }}
                >
                  <MapPin className="size-[18px] shrink-0 text-accent-600" />
                  <span>{city}</span>
                </button>
              ))
            ) : (
              <div className="rounded-chip bg-surface-empty p-4.5 text-ink-500">
                Choose a region before narrowing to a city.
              </div>
            )}
          </div>
        </div>
      ) : null}

      {activeDropdown === 'specialty' ? (
        <div className="mx-auto mt-3 w-[min(860px,100%)] animate-panel-in rounded-panel border border-white/95 bg-surface-panel-strong p-4.5 shadow-panel backdrop-blur-[16px]">
          <div className="mb-3.5 flex items-center justify-between text-panel-header text-ink-500">
            <span>Specialties</span>
            <span>{metadata?.specialties.length ?? 0}</span>
          </div>
          <div className="grid max-h-[320px] grid-cols-2 gap-2 overflow-auto max-[920px]:grid-cols-1">
            {metadata?.specialties.map((specialty) => {
              const selected = filters.specialties.includes(specialty);
              return (
                <button
                  type="button"
                  key={specialty}
                  className={`flex items-center gap-3 rounded-chip border px-4 py-3.5 text-left transition duration-150 ${
                    selected
                      ? 'border-border-highlight bg-surface-accent-strong'
                      : 'border-border-option bg-white hover:-translate-y-px hover:border-border-highlight hover:bg-surface-accent-strong'
                  }`}
                  onClick={() => onToggleSpecialty(specialty)}
                >
                  <Stethoscope className="size-[18px] shrink-0 text-accent-600" />
                  <span>{formatLabel(specialty)}</span>
                </button>
              );
            })}
          </div>
          {filters.specialties.length > 0 ? (
            <div className="mt-4 flex flex-wrap gap-2.5">
              {filters.specialties.map((specialty) => (
                <button
                  type="button"
                  key={specialty}
                  className="inline-flex items-center gap-2 rounded-full bg-surface-accent px-3.5 py-2 font-semibold text-accent-700"
                  onClick={() => onToggleSpecialty(specialty)}
                >
                  {formatLabel(specialty)}
                  <X className="size-3.5" />
                </button>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
