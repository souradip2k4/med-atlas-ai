import type { ReactNode } from 'react';

import { formatLabel, getAddressLines } from '../lib/format';
import type { FacilityProfile } from '../lib/types';
import {
  CloseIcon,
  GlobeIcon,
  LocationPinIcon,
  PhoneIcon,
  SpecialtyIcon,
} from './Icons';

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
    <section className="detail-section">
      <h3>{title}</h3>
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
      <div className="detail-chip-cloud">
        {values.map((item) => (
          <span className="detail-chip" key={item}>
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
    <aside className="detail-panel">
      <div className="detail-panel__header">
        <button type="button" className="detail-panel__close" onClick={onClose}>
          <CloseIcon />
        </button>
        <div>
          <div className="detail-panel__eyebrow">Facility profile</div>
          <h2>{profile?.facility_name ?? 'Loading facility details'}</h2>
        </div>
      </div>

      {isLoading ? (
        <div className="detail-panel__placeholder">
          <strong>Loading facility profile</strong>
          <p>Fetching the full medical, contact, and location profile for this facility.</p>
        </div>
      ) : null}

      {isError ? (
        <div className="detail-panel__placeholder is-error">
          <strong>Could not load this facility</strong>
          <p>{errorMessage}</p>
        </div>
      ) : null}

      {!isLoading && !isError && profile ? (
        <div className="detail-panel__body">
          <div className="detail-hero">
            <div>
              <span className="detail-badge">
                {formatLabel(profile.facility_type) || 'Facility'}
              </span>
              {profile.operator_type ? (
                <span className="detail-secondary-badge">
                  {formatLabel(profile.operator_type)}
                </span>
              ) : null}
            </div>
            <p>
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
                <div className="detail-row">
                  <LocationPinIcon className="detail-row__icon" />
                  <div>
                    {addressLines.map((line) => (
                      <div key={line}>{line}</div>
                    ))}
                    {locationParts.length > 0 ? <div>{locationParts.join(', ')}</div> : null}
                  </div>
                </div>
              ) : locationParts.length > 0 ? (
                <div className="detail-row">
                  <LocationPinIcon className="detail-row__icon" />
                  <div>{locationParts.join(', ')}</div>
                </div>
              ) : null}
            </Section>
          ) : null}

          {profile.phone_numbers?.length || profile.email || profile.websites?.length ? (
            <Section title="Contact">
              {profile.phone_numbers?.map((phone) => (
                <div className="detail-row" key={phone}>
                  <PhoneIcon className="detail-row__icon" />
                  <div>{phone}</div>
                </div>
              ))}
              {profile.email ? (
                <div className="detail-row">
                  <SpecialtyIcon className="detail-row__icon" />
                  <div>{profile.email}</div>
                </div>
              ) : null}
              {profile.websites?.map((website) => (
                <div className="detail-row" key={website}>
                  <GlobeIcon className="detail-row__icon" />
                  <div>{website}</div>
                </div>
              ))}
            </Section>
          ) : null}

          {profile.affiliation_types?.length ? (
            <Section title="Affiliations">
              <div className="detail-chip-cloud">
                {profile.affiliation_types.map((item) => (
                  <span className="detail-chip" key={item}>
                    {formatLabel(item)}
                  </span>
                ))}
              </div>
            </Section>
          ) : null}

          {profile.capacity || profile.no_doctors || profile.year_established ? (
            <Section title="Operational details">
              {profile.capacity ? <div className="detail-stat">Capacity: {profile.capacity}</div> : null}
              {profile.no_doctors ? (
                <div className="detail-stat">Doctors: {profile.no_doctors}</div>
              ) : null}
              {profile.year_established ? (
                <div className="detail-stat">Established: {profile.year_established}</div>
              ) : null}
            </Section>
          ) : null}
        </div>
      ) : null}
    </aside>
  );
}
