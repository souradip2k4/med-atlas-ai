import { ArrowLeft, MapPin } from 'lucide-react';
import { useUIStore } from '../store/ui-store';
import type { ExtractedMapMarker } from '../lib/types';

interface ChatMappedFacilitiesViewProps {
  entryId: string;
  facilities: ExtractedMapMarker[];
  onClose: () => void;
}

function formatCoordinate(value: number): string {
  return value.toFixed(4);
}

export function ChatMappedFacilitiesView({
  entryId,
  facilities,
  onClose,
}: ChatMappedFacilitiesViewProps) {
  const { setExtractedMapMarkers, setSelectedFacilityId } = useUIStore();
  const nameCounts = facilities.reduce<Record<string, number>>((accumulator, facility) => {
    accumulator[facility.name] = (accumulator[facility.name] ?? 0) + 1;
    return accumulator;
  }, {});

  const handleFacilityClick = (facility: ExtractedMapMarker) => {
    setExtractedMapMarkers(entryId, facilities);
    setSelectedFacilityId(facility.id);
  };

  return (
    <div className="flex h-full flex-col animate-chat-slide overflow-hidden">
      <div className="flex items-center gap-3 border-b border-border-app bg-[var(--color-chat-header)] px-5 py-4 backdrop-blur-[10px]">
        <button
          type="button"
          onClick={onClose}
          className="flex items-center justify-center rounded-full p-2 text-ink-500 transition hover:bg-surface-accent hover:text-accent-700"
          aria-label="Back to chat"
        >
          <ArrowLeft className="size-5" />
        </button>
        <div>
          <h3 className="font-semibold leading-tight text-ink-900">Facilities found in this response</h3>
          <p className="text-[0.8rem] text-ink-500">
            {facilities.length} mapped facilit{facilities.length === 1 ? 'y' : 'ies'}
          </p>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto bg-[var(--color-chat-body)] px-5 py-4 scrollbar-thin">
        <div className="grid gap-2 rounded-[26px] border border-[var(--color-chat-citation-rail-border)] bg-[var(--color-chat-citation-rail)] p-2 shadow-[var(--color-chat-citation-rail-shadow)]">
          {facilities.map((facility) => {
            const hasDuplicateName = (nameCounts[facility.name] ?? 0) > 1;

            return (
              <button
                key={`${facility.id}-${facility.latitude}-${facility.longitude}`}
                type="button"
                onClick={() => handleFacilityClick(facility)}
                className="group w-full rounded-[20px] border border-[var(--color-chat-citation-card-border)] bg-surface-panel p-3.5 text-left shadow-[var(--color-chat-citation-card-shadow)] transition-colors hover:bg-surface-panel-soft/40"
              >
                <div className="flex items-start gap-2">
                  <MapPin className="mt-0.5 size-3.5 shrink-0 text-accent-700 opacity-80" />
                  <div className="min-w-0">
                    <div className="text-[0.9rem] font-semibold text-accent-700">{facility.name}</div>
                    {hasDuplicateName ? (
                      <div className="mt-1 text-[0.74rem] text-ink-500">
                        {formatCoordinate(facility.latitude)}, {formatCoordinate(facility.longitude)}
                      </div>
                    ) : null}
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
