import { useEffect } from 'react';
import {
  Check,
  CircleOff,
  Hospital,
  ShieldPlus,
  SlidersHorizontal,
  Users2,
  X,
} from 'lucide-react';

import { countActiveAdvancedFilters, formatLabel } from '../lib/format';
import type { MapMetadata, SearchFilters } from '../lib/types';

interface AdvancedFiltersModalProps {
  open: boolean;
  metadata: MapMetadata | undefined;
  filters: SearchFilters;
  onClose: () => void;
  onAdvancedFilterChange: (
    key: 'facilityType' | 'operatorType' | 'organizationType',
    value: string,
  ) => void;
  onAffiliationToggle: (value: string) => void;
  onResetAdvancedFilters: () => void;
}

function FilterChoice({
  label,
  selected,
  icon: Icon,
  onClick,
}: {
  label: string;
  selected: boolean;
  icon?: typeof Hospital;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={
        selected
          ? 'flex min-h-24 items-center justify-between rounded-[22px] border border-border-highlight bg-surface-accent-strong px-4 py-3 text-left shadow-card-active'
          : 'flex min-h-24 items-center justify-between rounded-[22px] border border-border-soft bg-surface-card px-4 py-3 text-left shadow-card transition hover:border-border-highlight-soft hover:bg-surface-card-strong'
      }
      onClick={onClick}
    >
      <div className="min-w-0">
        {Icon ? (
          <span
            className={
              selected
                ? 'mb-3 inline-flex size-10 items-center justify-center rounded-2xl bg-surface-accent text-accent-700'
                : 'mb-3 inline-flex size-10 items-center justify-center rounded-2xl bg-surface-filter text-ink-500'
            }
          >
            <Icon className="size-[18px]" />
          </span>
        ) : null}
        <div className="text-[0.98rem] font-medium leading-6 text-ink-900">{label}</div>
      </div>

      <span
        className={
          selected
            ? 'ml-3 inline-flex size-6 shrink-0 items-center justify-center rounded-full bg-accent-600 text-white'
            : 'ml-3 inline-flex size-6 shrink-0 items-center justify-center rounded-full border border-border-field bg-surface-card-strong text-transparent'
        }
      >
        <Check className="size-3.5" strokeWidth={2.7} />
      </span>
    </button>
  );
}

function FilterTag({
  label,
  selected,
  onClick,
}: {
  label: string;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={
        selected
          ? 'rounded-full border border-border-highlight bg-surface-accent px-4 py-2.5 text-[0.94rem] font-medium text-accent-700 shadow-chip'
          : 'rounded-full border border-border-field bg-surface-card-strong px-4 py-2.5 text-[0.94rem] font-medium text-ink-700 transition hover:border-border-highlight-soft hover:bg-surface-card'
      }
      onClick={onClick}
    >
      {label}
    </button>
  );
}

export function AdvancedFiltersModal({
  open,
  metadata,
  filters,
  onClose,
  onAdvancedFilterChange,
  onAffiliationToggle,
  onResetAdvancedFilters,
}: AdvancedFiltersModalProps) {
  const advancedFilterCount = countActiveAdvancedFilters(filters);

  useEffect(() => {
    if (!open) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [open, onClose]);

  if (!open) {
    return null;
  }

  return (
    <div
      className="absolute inset-0 z-[38] flex items-center justify-center bg-[var(--color-modal-backdrop)] px-4 py-6 backdrop-blur-[6px]"
      onClick={onClose}
    >
      <div
        className="grid max-h-[min(82dvh,860px)] w-[min(780px,calc(100%-20px))] grid-rows-[auto_minmax(0,1fr)_auto] overflow-hidden rounded-[30px] border border-border-white-soft bg-surface-panel-strong shadow-[0_36px_72px_rgba(19,44,88,0.22)] max-[720px]:w-[calc(100%-10px)]"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-5 border-b border-b-border-header px-6 py-5">
          <div className="flex items-start gap-4">
            <button
              type="button"
              className="inline-flex size-11 shrink-0 items-center justify-center rounded-full border border-border-field bg-surface-card-strong text-ink-700"
              onClick={onClose}
            >
              <X className="size-5" />
            </button>
            <div>
              <div className="text-[1.25rem] font-semibold text-ink-900">
                Healthcare Facility Filters
              </div>
              <p className="mt-1 text-[0.96rem] text-ink-500">
                Refine the facilities shown on the map without hiding the results list.
              </p>
            </div>
          </div>

          <span className="inline-flex h-8 min-w-8 items-center justify-center rounded-full bg-surface-accent px-2.5 text-sm font-bold text-accent-700">
            {advancedFilterCount}
          </span>
        </div>

        <div className="overflow-auto px-6 py-6">
          <div className="grid gap-7">
            <section>
              <div className="mb-3 flex items-center gap-2 text-[0.8rem] uppercase tracking-[0.12em] text-ink-500">
                <SlidersHorizontal className="size-4 text-accent-600" />
                Facility Type
              </div>
              <div className="grid grid-cols-2 gap-3 max-[640px]:grid-cols-1">
                <FilterChoice
                  label="All facility types"
                  icon={CircleOff}
                  selected={!filters.facilityType}
                  onClick={() => onAdvancedFilterChange('facilityType', '')}
                />
                {metadata?.facility_types.map((item) => (
                  <FilterChoice
                    key={item}
                    label={formatLabel(item)}
                    icon={Hospital}
                    selected={filters.facilityType === item}
                    onClick={() => onAdvancedFilterChange('facilityType', item)}
                  />
                ))}
              </div>
            </section>

            <section className="border-t border-t-border-header pt-6">
              <div className="mb-3 text-[0.8rem] uppercase tracking-[0.12em] text-ink-500">
                Operator Type
              </div>
              <div className="grid grid-cols-2 gap-3 max-[640px]:grid-cols-1">
                <FilterChoice
                  label="All operators"
                  icon={CircleOff}
                  selected={!filters.operatorType}
                  onClick={() => onAdvancedFilterChange('operatorType', '')}
                />
                {metadata?.operator_types.map((item) => (
                  <FilterChoice
                    key={item}
                    label={formatLabel(item)}
                    icon={ShieldPlus}
                    selected={filters.operatorType === item}
                    onClick={() => onAdvancedFilterChange('operatorType', item)}
                  />
                ))}
              </div>
            </section>

            <section className="border-t border-t-border-header pt-6">
              <div className="mb-3 text-[0.8rem] uppercase tracking-[0.12em] text-ink-500">
                Organization
              </div>
              <div className="grid grid-cols-2 gap-3 max-[640px]:grid-cols-1">
                <FilterChoice
                  label="All organizations"
                  icon={CircleOff}
                  selected={!filters.organizationType}
                  onClick={() => onAdvancedFilterChange('organizationType', '')}
                />
                {metadata?.organization_types.map((item) => (
                  <FilterChoice
                    key={item}
                    label={formatLabel(item)}
                    icon={Users2}
                    selected={filters.organizationType === item}
                    onClick={() => onAdvancedFilterChange('organizationType', item)}
                  />
                ))}
              </div>
            </section>

            <section className="border-t border-t-border-header pt-6">
              <div className="mb-1 text-[0.8rem] uppercase tracking-[0.12em] text-ink-500">
                Affiliation Types
              </div>
              <p className="mb-4 text-[0.94rem] text-ink-500">
                Choose one or more network affiliations to refine the result set.
              </p>
              <div className="flex flex-wrap gap-2.5">
                {metadata?.affiliation_types.map((item) => {
                  const selected = filters.affiliationTypes.includes(item);
                  return (
                    <FilterTag
                      key={item}
                      label={formatLabel(item)}
                      selected={selected}
                      onClick={() => onAffiliationToggle(item)}
                    />
                  );
                })}
              </div>
            </section>
          </div>
        </div>

        <div className="flex items-center justify-end gap-3 border-t border-t-border-header px-6 py-4">
          <button
            type="button"
            className="rounded-full border border-border-field bg-surface-card-strong px-4 py-2.5 text-[0.96rem] font-medium text-ink-600 transition hover:border-border-highlight-soft hover:text-accent-700"
            onClick={onResetAdvancedFilters}
            disabled={advancedFilterCount === 0}
          >
            Clear All
          </button>
          <button
            type="button"
            className="rounded-[16px] bg-accent-600 px-5 py-3 text-[0.98rem] font-semibold text-white shadow-[0_14px_24px_rgba(53,103,190,0.2)] transition hover:bg-accent-700"
            onClick={onClose}
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
}
