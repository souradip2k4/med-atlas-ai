import type { ReactNode } from 'react';
import type { LucideIcon } from 'lucide-react';
import {
  Building2,
  ChevronRight,
  Globe,
  HeartPulse,
  Mail,
  MapPin,
  Phone,
  Stethoscope,
  Users,
  X,
} from 'lucide-react';

import { formatLabel, getAddressLines } from '../lib/format';
import type { FacilityProfile, FacilitySummary } from '../lib/types';

interface FacilityDetailsPanelProps {
  profile: FacilityProfile | null | undefined;
  preview: FacilitySummary | null;
  selectedFacilityId: string | null;
  isLoading: boolean;
  isError: boolean;
  errorMessage: string;
  onClose: () => void;
}

function DetailSection({
  icon: Icon,
  title,
  children,
}: {
  icon: LucideIcon;
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="mt-7 border-t border-t-border-header pt-7 first:mt-0 first:border-t-0 first:pt-0">
      <div className="mb-4 flex items-center gap-3">
        <Icon className="size-5 text-ink-700" strokeWidth={1.9} />
        <h3 className="text-[1.12rem] font-semibold text-ink-900">{title}</h3>
      </div>
      {children}
    </section>
  );
}

function ArrowList({ items }: { items: string[] }) {
  return (
    <ul className="grid gap-3">
      {items.map((item) => (
        <li key={item} className="flex items-start gap-3 text-[1.02rem] leading-8 text-ink-600">
          <ChevronRight className="mt-1 size-4 shrink-0 text-accent-600" strokeWidth={2.4} />
          <span>{formatLabel(item)}</span>
        </li>
      ))}
    </ul>
  );
}

function MetaRow({
  icon: Icon,
  children,
}: {
  icon: LucideIcon;
  children: ReactNode;
}) {
  return (
    <div className="mt-3 flex items-start gap-3 first:mt-0">
      <Icon className="mt-0.5 size-[18px] shrink-0 text-accent-600" />
      <div className="text-[1rem] leading-7 text-ink-600">{children}</div>
    </div>
  );
}

function DetailSkeleton() {
  return (
    <div className="overflow-auto bg-surface-panel-strong px-7 pb-8 pt-6">
      <div className="rounded-[28px] border border-border-soft bg-surface-card p-2.5 shadow-card">
        <div className="mb-5 flex gap-2.5">
          <div className="skeleton-block h-10 w-28 rounded-full" />
          <div className="skeleton-block h-10 w-24 rounded-full" />
        </div>
        <div className="grid gap-3">
          <div className="skeleton-block h-4.5 w-full rounded-full" />
          <div className="skeleton-block h-4.5 w-[90%] rounded-full" />
          <div className="skeleton-block h-4.5 w-[78%] rounded-full" />
        </div>
      </div>

      <div className="mt-7 border-t border-t-border-header pt-7">
        <div className="mb-4 flex items-center gap-3">
          <div className="skeleton-block h-5 w-5 rounded-full" />
          <div className="skeleton-block h-6 w-28 rounded-full" />
        </div>
        <div className="grid gap-3">
          <div className="skeleton-block h-4.5 w-full rounded-full" />
          <div className="skeleton-block h-4.5 w-[92%] rounded-full" />
          <div className="skeleton-block h-4.5 w-[68%] rounded-full" />
        </div>
      </div>

      <div className="mt-7 border-t border-t-border-header pt-7">
        <div className="mb-4 flex items-center gap-3">
          <div className="skeleton-block h-5 w-5 rounded-full" />
          <div className="skeleton-block h-6 w-24 rounded-full" />
        </div>
        <div className="grid gap-3">
          <div className="skeleton-block h-11 w-[86%] rounded-full" />
          <div className="skeleton-block h-11 w-[58%] rounded-full" />
          <div className="skeleton-block h-11 w-[74%] rounded-full" />
        </div>
      </div>
    </div>
  );
}

export function FacilityDetailsPanel({
  profile,
  preview,
  selectedFacilityId,
  isLoading,
  isError,
  errorMessage,
  onClose,
}: FacilityDetailsPanelProps) {
  if (!selectedFacilityId) {
    return null;
  }

  const displayName = profile?.facility_name ?? preview?.facility_name ?? 'Loading facility details';
  const facilityType = formatLabel(profile?.facility_type ?? preview?.facility_type) || 'Facility';
  const operatorType = formatLabel(profile?.operator_type ?? preview?.operator_type);
  const description =
    profile?.description ??
    preview?.description ??
    'No available description data';

  const addressLines = profile ? getAddressLines(profile) : [];
  const locationParts = profile
    ? [profile.city, profile.state, profile.country].filter(
        (item): item is string => Boolean(item),
      )
    : [preview?.city, preview?.state].filter((item): item is string => Boolean(item));

  // const overviewParts = [profile?.description, profile?.mission_statement]
  //   .filter((item): item is string => Boolean(item && item.trim()));

  const operationalRows = [
    profile?.capacity ? { label: 'Capacity', value: String(profile.capacity) } : null,
    profile?.no_doctors ? { label: 'Doctors', value: String(profile.no_doctors) } : null,
    profile?.year_established
      ? { label: 'Established', value: String(profile.year_established) }
      : null,
  ].filter((item): item is { label: string; value: string } => Boolean(item));

  return (
    <aside className="absolute right-6 top-[118px] z-[28] grid max-h-[calc(100dvh-146px)] w-[min(540px,calc(100%-48px))] grid-rows-[auto_minmax(0,1fr)] overflow-hidden rounded-[30px] border border-border-white-soft bg-surface-panel-strong shadow-panel-strong backdrop-blur-[18px] animate-panel-in max-[920px]:bottom-3 max-[920px]:right-3 max-[920px]:top-auto max-[920px]:max-h-[58dvh] max-[920px]:w-[calc(100%-24px)]">
      <div className="grid grid-cols-[auto_minmax(0,1fr)] items-start gap-4 border-b border-b-border-header bg-surface-panel-strong px-3 pb-4 pt-5 shadow-overlay">
        <button
          type="button"
          className="inline-flex size-11 shrink-0 items-center justify-center bg-surface-card-strong text-ink-700 rounded-full hover:bg-surface-filter-strong hover:text-accent-700"
          onClick={onClose}
        >
          <X className="size-[19px]" />
        </button>

        <div className="min-w-0 pr-2">
          <div className="text-lg font-medium text-accent-600">
            Facility Profile
          </div>
          <h2 className="mt-1.5 break-words text-[1.06rem] font-semibold leading-tight text-ink-900">
            {displayName}
          </h2>
        </div>
      </div>

      {isLoading ? <DetailSkeleton /> : null}

      {isError ? (
        <div className="grid gap-2 bg-surface-panel-strong px-7 py-7 text-ink-600">
          <strong className="text-ink-900">Could not load this facility</strong>
          <p>{errorMessage}</p>
        </div>
      ) : null}

      {!isLoading && !isError && (profile || preview) ? (
        <div className="overflow-auto bg-surface-panel-strong px-7 pb-8 pt-6">
          <div className="rounded-[28px] border border-border-soft bg-surface-card p-5 shadow-card">
            <div className="mb-5 flex flex-wrap gap-3">
              <span className="inline-flex items-center rounded-full bg-surface-accent px-4 py-2.5 text-[0.72rem] font-bold uppercase tracking-[0.12em] text-accent-700">
                {facilityType}
              </span>
              {operatorType ? (
                <span className="inline-flex items-center rounded-full bg-surface-teal px-4 py-2.5 text-[0.72rem] font-bold uppercase tracking-[0.12em] text-tone-teal">
                  {operatorType}
                </span>
              ) : null}
            </div>
            <p className="text-[1rem] leading-8 text-ink-600">{description}</p>
          </div>

          {/* <DetailSection icon={Globe} title="Overview">
            <div className="grid gap-4 text-[1.02rem] leading-8 text-ink-600">
              {(overviewParts.length > 0 ? overviewParts : [description]).map((item) => (
                <p key={item}>{item}</p>
              ))}
            </div>
          </DetailSection> */}

          {profile?.specialties?.length ? (
            <DetailSection icon={Stethoscope} title="Specialties">
              <ArrowList items={profile.specialties} />
            </DetailSection>
          ) : null}

          {profile?.procedures?.length ? (
            <DetailSection icon={HeartPulse} title="Procedures">
              <ArrowList items={profile.procedures} />
            </DetailSection>
          ) : null}

          {profile?.equipment?.length ? (
            <DetailSection icon={Building2} title="Equipment">
              <ArrowList items={profile.equipment} />
            </DetailSection>
          ) : null}

          {profile?.capabilities?.length ? (
            <DetailSection icon={HeartPulse} title="Capabilities">
              <ArrowList items={profile.capabilities} />
            </DetailSection>
          ) : null}

          {addressLines.length > 0 || locationParts.length > 0 ? (
            <DetailSection icon={MapPin} title="Location">
              {addressLines.length > 0 ? (
                <MetaRow icon={MapPin}>
                  <div>
                    {addressLines.map((line) => (
                      <div key={line}>{line}</div>
                    ))}
                    {locationParts.length > 0 ? <div>{locationParts.join(', ')}</div> : null}
                  </div>
                </MetaRow>
              ) : (
                <MetaRow icon={MapPin}>{locationParts.join(', ')}</MetaRow>
              )}
            </DetailSection>
          ) : null}

          {profile?.phone_numbers?.length || profile?.email || profile?.websites?.length ? (
            <DetailSection icon={Phone} title="Contact">
              {profile.phone_numbers?.map((phone) => (
                <MetaRow icon={Phone} key={phone}>
                  {phone}
                </MetaRow>
              ))}
              {profile.email ? <MetaRow icon={Mail}>{profile.email}</MetaRow> : null}
              {profile.websites?.map((website) => (
                <MetaRow icon={Globe} key={website}>
                  {website}
                </MetaRow>
              ))}
            </DetailSection>
          ) : null}

          {profile?.affiliation_types?.length ? (
            <DetailSection icon={Building2} title="Affiliations">
              <ArrowList items={profile.affiliation_types} />
            </DetailSection>
          ) : null}

          {operationalRows.length > 0 ? (
            <DetailSection icon={Users} title="Operational Details">
              <div className="grid gap-3 text-[1rem] text-ink-600">
                {operationalRows.map((row) => (
                  <div
                    key={row.label}
                    className="grid grid-cols-[minmax(0,1fr)_auto] gap-6 border-b border-b-border-header pb-3 last:border-b-0 last:pb-0"
                  >
                    <span className="text-ink-500">{row.label}</span>
                    <strong className="font-medium text-ink-900">{row.value}</strong>
                  </div>
                ))}
              </div>
            </DetailSection>
          ) : null}

          {/* {profile?.created_at || profile?.updated_at ? (
            <DetailSection icon={CalendarDays} title="Record Timeline">
              <div className="grid gap-3 text-[1rem] text-[#23384f]">
                {profile.created_at ? (
                  <div className="grid grid-cols-[minmax(0,1fr)_auto] gap-6 border-b border-b-border-header pb-3">
                    <span className="text-[#5b7390]">Created</span>
                    <strong className="font-medium text-[#1b2a3a]">{profile.created_at}</strong>
                  </div>
                ) : null}
                {profile.updated_at ? (
                  <div className="grid grid-cols-[minmax(0,1fr)_auto] gap-6">
                    <span className="text-[#5b7390]">Updated</span>
                    <strong className="font-medium text-[#1b2a3a]">{profile.updated_at}</strong>
                  </div>
                ) : null}
              </div>
            </DetailSection>
          ) : null} */}
        </div>
      ) : null}
    </aside>
  );
}
