import type { ReactNode } from 'react';
import { Globe, MapPin, Mail, Phone, X } from 'lucide-react';

import { formatLabel, getAddressLines } from '../lib/format';
import type { FacilityProfile } from '../lib/types';

interface FacilityDetailsPanelProps {
  profile: FacilityProfile | null | undefined;
  selectedFacilityId: string | null;
  isLoading: boolean;
  isError: boolean;
  errorMessage: string;
  onClose: () => void;
}

function Section({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="mt-4.5 first:mt-0">
      <h3 className="mb-3 text-base text-ink-900">{title}</h3>
      {children}
    </section>
  );
}

function InlineList({
  title,
  values,
}: {
  title: string;
  values: string[] | null;
}) {
  if (!values || values.length === 0) {
    return null;
  }

  return (
    <Section title={title}>
      <div className="flex flex-wrap gap-2">
        {values.map((item) => (
          <span
            className="rounded-full bg-surface-panel-strong px-3 py-2 text-ink-700 shadow-inset-soft"
            key={item}
          >
            {formatLabel(item)}
          </span>
        ))}
      </div>
    </Section>
  );
}

export function FacilityDetailsPanel({
  profile,
  selectedFacilityId,
  isLoading,
  isError,
  errorMessage,
  onClose,
}: FacilityDetailsPanelProps) {
  if (!selectedFacilityId) {
    return null;
  }

  const addressLines = profile ? getAddressLines(profile) : [];
  const locationParts = profile
    ? [profile.city, profile.state, profile.country].filter(
        (item): item is string => Boolean(item),
      )
    : [];

  return (
    <aside className="absolute right-7 top-[118px] z-[28] grid max-h-[calc(100dvh-148px)] w-[min(430px,calc(100%-56px))] grid-rows-[auto_minmax(0,1fr)] overflow-hidden rounded-[32px] border border-border-white-soft bg-surface-card shadow-panel-strong backdrop-blur-[18px] animate-panel-in max-[920px]:bottom-3 max-[920px]:right-3 max-[920px]:top-auto max-[920px]:max-h-[55dvh] max-[920px]:w-[calc(100%-24px)]">
      <div className="flex items-start gap-3.5 border-b border-b-border-header px-[22px] pb-4 pt-[22px]">
        <button
          type="button"
          className="inline-flex size-9.5 shrink-0 items-center justify-center rounded-full border-0 bg-surface-filter text-ink-700"
          onClick={onClose}
        >
          <X className="size-[18px]" />
        </button>
        <div>
          <div className="mb-1.5 text-detail-badge uppercase tracking-[0.14em] text-accent-600">
            Facility profile
          </div>
          <h2 className="text-[1.9rem] leading-[1.04] text-ink-900">
            {profile?.facility_name ?? 'Loading facility details'}
          </h2>
        </div>
      </div>

      {isLoading ? (
        <div className="grid gap-2 rounded-panel bg-surface-panel p-6 text-ink-600 shadow-inset-soft">
          <strong className="text-ink-900">Loading facility profile</strong>
          <p>Fetching the full medical, contact, and location profile for this facility.</p>
        </div>
      ) : null}

      {isError ? (
        <div className="grid gap-2 rounded-panel bg-surface-error p-6 text-ink-600 shadow-inset-soft">
          <strong className="text-ink-900">Could not load this facility</strong>
          <p>{errorMessage}</p>
        </div>
      ) : null}

      {!isLoading && !isError && profile ? (
        <div className="overflow-auto px-[22px] pb-6 pt-5">
          <div className="mb-4.5 rounded-pill bg-[linear-gradient(145deg,rgba(240,247,255,0.9),rgba(255,255,255,0.95))] p-4.5 shadow-inset-field">
            <div>
              <span className="inline-flex items-center rounded-full bg-surface-accent px-3 py-2 text-detail-badge font-bold uppercase tracking-[0.08em] text-accent-700">
                {formatLabel(profile.facility_type) || 'Facility'}
              </span>
              {profile.operator_type ? (
                <span className="ml-2 inline-flex items-center rounded-full bg-surface-teal px-3 py-2 text-detail-badge font-bold uppercase tracking-[0.08em] text-tone-teal">
                  {formatLabel(profile.operator_type)}
                </span>
              ) : null}
            </div>
            <p className="mt-3 text-ink-600">
              {profile.description ||
                'Detailed profile loaded. Fields without verified values are hidden to keep this view clean.'}
            </p>
          </div>

          <InlineList title="Specialties" values={profile.specialties} />
          <InlineList title="Procedures" values={profile.procedures} />
          <InlineList title="Equipment" values={profile.equipment} />
          <InlineList title="Capabilities" values={profile.capabilities} />

          {addressLines.length > 0 || locationParts.length > 0 ? (
            <Section title="Location">
              {addressLines.length > 0 ? (
                <div className="flex items-start gap-3 text-ink-700">
                  <MapPin className="size-[18px] shrink-0 text-accent-600" />
                  <div>
                    {addressLines.map((line) => (
                      <div key={line}>{line}</div>
                    ))}
                    {locationParts.length > 0 ? <div>{locationParts.join(', ')}</div> : null}
                  </div>
                </div>
              ) : locationParts.length > 0 ? (
                <div className="flex items-start gap-3 text-ink-700">
                  <MapPin className="size-[18px] shrink-0 text-accent-600" />
                  <div>{locationParts.join(', ')}</div>
                </div>
              ) : null}
            </Section>
          ) : null}

          {profile.phone_numbers?.length || profile.email || profile.websites?.length ? (
            <Section title="Contact">
              {profile.phone_numbers?.map((phone) => (
                <div className="mt-2.5 flex items-start gap-3 text-ink-700 first:mt-0" key={phone}>
                  <Phone className="size-[18px] shrink-0 text-accent-600" />
                  <div>{phone}</div>
                </div>
              ))}
              {profile.email ? (
                <div className="mt-2.5 flex items-start gap-3 text-ink-700">
                  <Mail className="size-[18px] shrink-0 text-accent-600" />
                  <div>{profile.email}</div>
                </div>
              ) : null}
              {profile.websites?.map((website) => (
                <div className="mt-2.5 flex items-start gap-3 text-ink-700" key={website}>
                  <Globe className="size-[18px] shrink-0 text-accent-600" />
                  <div>{website}</div>
                </div>
              ))}
            </Section>
          ) : null}

          {profile.affiliation_types?.length ? (
            <Section title="Affiliations">
              <div className="flex flex-wrap gap-2">
                {profile.affiliation_types.map((item) => (
                  <span
                    className="rounded-full bg-surface-panel-strong px-3 py-2 text-ink-700 shadow-inset-soft"
                    key={item}
                  >
                    {formatLabel(item)}
                  </span>
                ))}
              </div>
            </Section>
          ) : null}

          {profile.capacity || profile.no_doctors || profile.year_established ? (
            <Section title="Operational details">
              {profile.capacity ? <div className="text-ink-700">Capacity: {profile.capacity}</div> : null}
              {profile.no_doctors ? (
                <div className="text-ink-700">Doctors: {profile.no_doctors}</div>
              ) : null}
              {profile.year_established ? (
                <div className="text-ink-700">Established: {profile.year_established}</div>
              ) : null}
            </Section>
          ) : null}
        </div>
      ) : null}
    </aside>
  );
}
