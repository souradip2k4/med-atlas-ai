import { ArrowLeft, Network, MapPin } from 'lucide-react';
import { useUIStore } from '../store/ui-store';
import type { AgentCitations } from '../lib/types';

interface ChatCitationsViewProps {
  citations: AgentCitations;
  onClose: () => void;
}

export function ChatCitationsView({ citations, onClose }: ChatCitationsViewProps) {
  const { setSelectedFacilityId } = useUIStore();

  const handleFacilityClick = (facilityId: string) => {
    setSelectedFacilityId(facilityId);
  };

  return (
    <div className="flex h-full flex-col animate-chat-slide overflow-hidden">
      <div className="flex items-center gap-3 border-b border-border-app px-5 py-4 bg-surface-panel-strong/98">
        <button
          type="button"
          onClick={onClose}
          className="flex items-center justify-center rounded-full p-2 text-ink-500 transition hover:bg-surface-accent hover:text-accent-700"
          aria-label="Back to chat"
        >
          <ArrowLeft className="size-5" />
        </button>
        <div>
          <h3 className="font-semibold text-ink-900 leading-tight">Sources for this response</h3>
          <p className="text-[0.8rem] text-ink-500">
            {citations.summary.total_sources} fact{citations.summary.total_sources === 1 ? '' : 's'} referenced
          </p>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-5 py-4 scrollbar-thin">
        {citations.steps.map((step, idx) => (
          <div key={idx} className="mb-6 last:mb-2">
            <div className="mb-3">
              <div className="inline-flex items-center gap-1.5 rounded-full bg-surface-filter px-2.5 py-1 text-[0.75rem] font-bold text-ink-600 mb-1">
                <Network className="size-3" strokeWidth={2.5} />
                {step.tool_name}
              </div>
              <p className="text-ui-sm text-ink-900 bg-surface-empty p-2.5 rounded-lg border border-border-panel font-medium">
                "{step.query_used}"
              </p>
            </div>

            <div className="grid gap-2 outline outline-1 outline-border-panel rounded-card p-1.5 bg-white">
              {step.sources?.map((source, sIdx) => (
                <div key={sIdx} className="rounded-xl bg-surface-panel-soft p-3 hover:bg-surface-accent-soft transition-colors group">
                  <div className="flex items-start justify-between gap-2 mb-1.5">
                    {source.facility_name ? (
                      <button
                        type="button"
                        onClick={() => source.facility_id && handleFacilityClick(source.facility_id)}
                        className="text-[0.85rem] font-semibold text-accent-700 text-left hover:underline flex items-start gap-1 p-0 bg-transparent border-0"
                      >
                        <MapPin className="size-3 mt-0.5 shrink-0 opacity-70" />
                        <span>{source.facility_name}</span>
                      </button>
                    ) : (
                      <strong className="text-[0.85rem] font-semibold text-ink-900">
                        {source.source_type}
                      </strong>
                    )}
                    {source.fact_type && (
                      <span className="shrink-0 rounded bg-white px-1.5 py-0.5 text-[0.65rem] font-bold uppercase tracking-wider text-ink-500 shadow-sm outline outline-1 outline-border-app">
                        {source.fact_type}
                      </span>
                    )}
                  </div>
                  <div className="text-[0.8rem] text-ink-700 leading-relaxed line-clamp-3 group-hover:line-clamp-none transition-all">
                    {source.excerpt}
                  </div>
                </div>
              ))}
              
              {(!step.sources || step.sources.length === 0) && (
                <div className="p-3 text-[0.8rem] text-ink-500 italic text-center">
                  No direct sources captured in tracking for this step.
                </div>
              )}
            </div>
          </div>
        ))}
      </div>

      <div className="border-t border-border-app bg-surface-panel-soft px-5 py-3 text-[0.75rem] text-ink-500">
        {citations.summary?.tools_used?.length > 0 && (
          <div className="mb-1.5 flex flex-wrap gap-x-3 gap-y-1">
            <strong>Tools used:</strong>
            {citations.summary.tools_used.join(', ')}
          </div>
        )}
        {citations.summary?.tables_accessed?.length > 0 && (
          <div className="flex flex-wrap gap-x-3 gap-y-1">
            <strong>Tables accessed:</strong>
            {citations.summary.tables_accessed.join(', ')}
          </div>
        )}
      </div>
    </div>
  );
}
