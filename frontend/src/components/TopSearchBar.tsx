import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import type { CSSProperties, ReactNode } from 'react';
import { LoaderCircle, MapPinned, MapPin, MoonStar, Search, Stethoscope, SunMedium, X } from 'lucide-react';
import type { ThemeMode } from '../lib/types';

import { formatLabel } from '../lib/format';
import type { DropdownKey, MapMetadata, SearchFilters } from '../lib/types';

interface TopSearchBarProps {
  metadata: MapMetadata | undefined;
  filters: SearchFilters;
  resolvedTheme: ThemeMode;
  activeDropdown: DropdownKey;
  isFacilitySearchLoading?: boolean;
  onDropdownChange: (dropdown: DropdownKey) => void;
  onThemeToggle: () => void;
  onFacilitySearch: (facilityName: string) => void;
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
      className={`${buttonClass} disabled:cursor-not-allowed disabled:opacity-55 w-full`}
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
          className="inline-flex size-6.5 items-center justify-center rounded-full border border-border-white-soft bg-surface-card-strong text-accent-600 shadow-chip"
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
  resolvedTheme,
  activeDropdown,
  isFacilitySearchLoading = false,
  onDropdownChange,
  onThemeToggle,
  onFacilitySearch,
  onRegionSelect,
  onCitySelect,
  onToggleSpecialty,
  onClearRegion,
  onClearCity,
  onClearSpecialties,
}: TopSearchBarProps) {
  const [dropdownStyle, setDropdownStyle] = useState<CSSProperties>();
  const [searchOpen, setSearchOpen] = useState(false);
  const [facilitySearchValue, setFacilitySearchValue] = useState('');
  const shellRef = useRef<HTMLDivElement | null>(null);
  const frameRef = useRef<HTMLDivElement | null>(null);
  const facilitySearchInputRef = useRef<HTMLInputElement | null>(null);
  const regionRef = useRef<HTMLDivElement | null>(null);
  const cityRef = useRef<HTMLDivElement | null>(null);
  const specialtyRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!shellRef.current?.contains(event.target as Node)) {
        onDropdownChange(null);
        setSearchOpen(false);
      }
    }

    document.addEventListener('mousedown', handlePointerDown);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
    };
  }, [onDropdownChange]);

  const cityOptions = metadata?.cities_by_region[filters.region] ?? [];
  const specialtyCount = filters.specialties.length;

  useEffect(() => {
    if (!searchOpen) {
      return;
    }

    const frameId = window.requestAnimationFrame(() => {
      facilitySearchInputRef.current?.focus();
      facilitySearchInputRef.current?.select();
    });

    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [searchOpen]);

  useLayoutEffect(() => {
    if (!activeDropdown) {
      return;
    }

    const updateDropdownStyle = () => {
      const frame = frameRef.current;
      const target =
        activeDropdown === 'region'
          ? regionRef.current
          : activeDropdown === 'city'
            ? cityRef.current
            : specialtyRef.current;

      if (!frame || !target) {
        setDropdownStyle(undefined);
        return;
      }

      const frameRect = frame.getBoundingClientRect();
      const targetRect = target.getBoundingClientRect();
      const maxWidth = frameRect.width;
      const desiredWidth =
        activeDropdown === 'specialty' ? Math.min(560, maxWidth) : Math.min(430, maxWidth);
      const preferredLeft = targetRect.left - frameRect.left;
      const clampedLeft = Math.min(
        Math.max(0, preferredLeft),
        Math.max(0, maxWidth - desiredWidth),
      );

      setDropdownStyle({
        left: `${clampedLeft}px`,
        width: `${desiredWidth}px`,
      });
    };

    updateDropdownStyle();
    window.addEventListener('resize', updateDropdownStyle);

    return () => {
      window.removeEventListener('resize', updateDropdownStyle);
    };
  }, [activeDropdown, filters.region, specialtyCount, cityOptions.length]);

  const handleFacilitySearchSubmit = () => {
    const trimmedValue = facilitySearchValue.trim();
    if (!trimmedValue) {
      return;
    }

    onDropdownChange(null);
    onFacilitySearch(trimmedValue);
    setSearchOpen(false);
  };

  return (
    <div
      className="absolute left-1/2 top-7 z-30 w-[min(880px,calc(100%-112px))] -translate-x-1/2 max-[920px]:top-3 max-[920px]:w-[calc(100%-24px)]"
      ref={shellRef}
    >
      <div className="relative mx-auto max-w-[700px]" ref={frameRef}>
        <div className="absolute left-[-58px] top-1/2 hidden -translate-y-1/2 min-[921px]:block">
          <div className="flex h-11 items-center overflow-hidden rounded-full border border-border-white-soft bg-surface-panel-strong text-accent-600 shadow-overlay backdrop-blur-[14px] transition-[background-color,box-shadow] duration-250 hover:bg-surface-card-strong">
            <button
              type="button"
              className={`inline-flex size-11 shrink-0 items-center justify-center transition ${
                searchOpen ? 'bg-surface-card-strong' : ''
              }`}
              onClick={() => {
                onDropdownChange(null);
                setSearchOpen((current) => !current);
              }}
              aria-label={searchOpen ? 'Close facility search' : 'Search by facility name'}
            >
              <Search className="size-5" strokeWidth={2.1} />
            </button>
          </div>
        </div>

        <div className="grid animate-chrome-in grid-cols-3 gap-2 rounded-[30px] border border-border-white-soft bg-surface-panel-strong shadow-search backdrop-blur-[18px]">
          <div ref={regionRef}>
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
          </div>
          <div ref={cityRef}>
            <FieldButton
              icon={<MapPin className="size-5" />}
              label="City"
              value={filters.city}
              active={activeDropdown === 'city'}
              disabled={!filters.region}
              onClick={() => onDropdownChange(activeDropdown === 'city' ? null : 'city')}
              onClear={filters.city ? onClearCity : undefined}
            />
          </div>
          <div ref={specialtyRef}>
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
        </div>

        <button
          type="button"
          className="absolute right-[-58px] top-1/2 hidden size-11 -translate-y-1/2 items-center justify-center rounded-full border border-border-white-soft bg-surface-panel-strong text-accent-600 shadow-overlay backdrop-blur-[14px] transition hover:bg-surface-card-strong min-[921px]:inline-flex"
          onClick={onThemeToggle}
          aria-label={`Switch to ${resolvedTheme === 'dark' ? 'light' : 'dark'} mode`}
        >
          {resolvedTheme === 'dark' ? (
            <SunMedium className="size-5" strokeWidth={2.1} />
          ) : (
            <MoonStar className="size-5" strokeWidth={2.1} />
          )}
        </button>

        <div className="mt-2 min-[921px]:hidden">
          <div className="flex items-center gap-2 rounded-[22px] border border-border-white-soft bg-surface-panel-strong px-3 py-2 shadow-overlay backdrop-blur-[14px]">
            <Search className="size-4.5 shrink-0 text-accent-600" />
            <input
              type="text"
              value={facilitySearchValue}
              onChange={(event) => setFacilitySearchValue(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  event.preventDefault();
                  handleFacilitySearchSubmit();
                }
              }}
              placeholder="Search facility by name"
              className="min-w-0 flex-1 border-0 bg-transparent text-[0.92rem] font-medium text-ink-900 placeholder:text-ink-400 focus:outline-none"
            />
            <button
              type="button"
              className="inline-flex size-8 shrink-0 items-center justify-center rounded-full bg-surface-accent text-accent-700 transition hover:bg-surface-accent-strong disabled:cursor-not-allowed disabled:opacity-55"
              onClick={handleFacilitySearchSubmit}
              disabled={!facilitySearchValue.trim() || isFacilitySearchLoading}
              aria-label="Search facility"
            >
              {isFacilitySearchLoading ? (
                <LoaderCircle className="size-4 animate-spin" />
              ) : (
                <Search className="size-4" />
              )}
            </button>
          </div>
        </div>

      {activeDropdown === 'region' ? (
        <div
          className="absolute top-[calc(100%+12px)] animate-panel-in rounded-panel border border-border-white-soft bg-surface-panel-strong p-4.5 shadow-panel backdrop-blur-[16px]"
          style={dropdownStyle}
        >
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
                    : 'border-border-option bg-surface-card hover:-translate-y-px hover:border-border-highlight hover:bg-surface-accent-strong'
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
        <div
          className="absolute top-[calc(100%+12px)] animate-panel-in rounded-panel border border-border-white-soft bg-surface-panel-strong p-4.5 shadow-panel backdrop-blur-[16px]"
          style={dropdownStyle}
        >
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
                      : 'border-border-option bg-surface-card hover:-translate-y-px hover:border-border-highlight hover:bg-surface-accent-strong'
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
        <div
          className="absolute top-[calc(100%+12px)] animate-panel-in rounded-panel border border-border-white-soft bg-surface-panel-strong p-4.5 shadow-panel backdrop-blur-[16px]"
          style={dropdownStyle}
        >
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
                      : 'border-border-option bg-surface-card hover:-translate-y-px hover:border-border-highlight hover:bg-surface-accent-strong'
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

      {searchOpen ? (
        <div className="absolute left-0 top-[calc(100%+12px)] hidden w-[min(460px,calc(100vw-140px))] animate-panel-in rounded-panel border border-border-white-soft bg-surface-panel-strong p-3.5 shadow-panel backdrop-blur-[16px] min-[921px]:block">
          <div className="flex items-center gap-2 rounded-[22px] border border-border-white-soft bg-surface-card px-3 py-2.5 shadow-inset-soft">
            <Search className="size-4.5 shrink-0 text-accent-600" />
            <input
              ref={facilitySearchInputRef}
              type="text"
              value={facilitySearchValue}
              onChange={(event) => setFacilitySearchValue(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  event.preventDefault();
                  handleFacilitySearchSubmit();
                }

                if (event.key === 'Escape') {
                  event.preventDefault();
                  setSearchOpen(false);
                }
              }}
              placeholder="Search facility by name"
              className="min-w-0 flex-1 border-0 bg-transparent px-0 py-0 text-[0.96rem] font-medium text-ink-900 outline-none ring-0 placeholder:text-ink-400 focus:outline-none focus:ring-0 focus-visible:outline-none focus-visible:ring-0"
            />
            {facilitySearchValue ? (
              <button
                type="button"
                className="inline-flex size-8 shrink-0 items-center justify-center rounded-full text-ink-400 transition hover:bg-surface-filter-strong hover:text-accent-600"
                onClick={() => setFacilitySearchValue('')}
                aria-label="Clear search query"
              >
                <X className="size-4" />
              </button>
            ) : null}
            <button
              type="button"
              className="inline-flex size-9 shrink-0 items-center justify-center rounded-full bg-surface-accent text-accent-700 transition hover:bg-surface-accent-strong disabled:cursor-not-allowed disabled:opacity-55"
              onClick={handleFacilitySearchSubmit}
              disabled={!facilitySearchValue.trim() || isFacilitySearchLoading}
              aria-label="Search facility"
            >
              {isFacilitySearchLoading ? (
                <LoaderCircle className="size-4 animate-spin" />
              ) : (
                <Search className="size-4" />
              )}
            </button>
          </div>
        </div>
      ) : null}
      </div>
    </div>
  );
}
