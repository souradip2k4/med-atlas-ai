import { useState } from 'react';
import { ArrowLeft, ChevronDown, Network, MapPin } from 'lucide-react';
import { useUIStore } from '../store/ui-store';
import type { AgentCitations } from '../lib/types';

interface ChatCitationsViewProps {
  citations: AgentCitations;
  onClose: () => void;
}

const DEFAULT_VISIBLE_SOURCE_COUNT = 10;

export function ChatCitationsView({ citations, onClose }: ChatCitationsViewProps) {
  const { setSelectedFacilityId } = useUIStore();
  const [expandedSteps, setExpandedSteps] = useState<Record<string, boolean>>({});

  const handleFacilityClick = (facilityId: string) => {
    setSelectedFacilityId(facilityId);
  };

  const toggleStepExpansion = (stepKey: string) => {
    setExpandedSteps((current) => ({
      ...current,
      [stepKey]: !current[stepKey],
    }));
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
          <h3 className="font-semibold text-ink-900 leading-tight">Sources for this response</h3>
          <p className="text-[0.8rem] text-ink-500">
            {citations.summary.total_sources} fact{citations.summary.total_sources === 1 ? '' : 's'} referenced
          </p>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto bg-[var(--color-chat-body)] px-5 py-4 scrollbar-thin">
        {citations.steps.map((step, idx) => {
          const stepKey = step.call_id || `${step.tool_name}-${idx}`;
          const sources = step.sources ?? [];
          const isExpanded = Boolean(expandedSteps[stepKey]);
          const hasOverflow = sources.length > DEFAULT_VISIBLE_SOURCE_COUNT;
          const visibleSources = isExpanded ? sources : sources.slice(0, DEFAULT_VISIBLE_SOURCE_COUNT);

          return (
            <div key={stepKey} className="mb-6 last:mb-2">
            <div className="mb-3">
              <div className="inline-flex items-center gap-1.5 rounded-full bg-surface-filter px-2.5 py-1 text-[0.75rem] font-bold text-ink-600 mb-1">
                <Network className="size-3" strokeWidth={2.5} />
                {step.tool_name}
              </div>
              <p className="rounded-lg border border-border-panel bg-surface-empty p-2.5 text-ui-sm font-medium text-ink-900">
                "{step.query_used}"
              </p>
            </div>

            <div className="grid gap-2 rounded-[26px] border border-[var(--color-chat-citation-rail-border)] bg-[var(--color-chat-citation-rail)] p-2 shadow-[var(--color-chat-citation-rail-shadow)]">
              {visibleSources.map((source, sIdx) => (
                <button
                  key={`${stepKey}-${sIdx}`}
                  type="button"
                  onClick={() => source.facility_id && handleFacilityClick(source.facility_id)}
                  disabled={!source.facility_id}
                  className="group w-full rounded-[20px] border border-[var(--color-chat-citation-card-border)] bg-surface-panel p-3.5 text-left shadow-[var(--color-chat-citation-card-shadow)] transition-colors hover:bg-surface-panel-soft/40 disabled:cursor-default disabled:hover:bg-[var(--color-chat-citation-card)]"
                >
                  <div className="flex items-start justify-between gap-2 mb-1.5">
                    {source.facility_name ? (
                      <div className="flex items-start gap-1 text-[0.85rem] font-semibold text-accent-700">
                        <MapPin className="size-3 mt-0.5 shrink-0 opacity-70" />
                        <span>{source.facility_name}</span>
                      </div>
                    ) : (
                      <strong className="text-[0.85rem] font-semibold text-ink-900">
                        {source.source_type}
                      </strong>
                    )}
                    {source.fact_type && (
                      <span className="shrink-0 rounded bg-[var(--color-chat-citation-badge-bg)] px-1.5 py-0.5 text-[0.65rem] font-bold uppercase tracking-wider text-ink-500 shadow-sm outline outline-1 outline-border-app">
                        {source.fact_type}
                      </span>
                    )}
                  </div>
                  {source.excerpt ? (
                    <div className="text-[0.8rem] text-ink-700 leading-relaxed line-clamp-3 group-hover:line-clamp-none transition-all">
                      {source.excerpt}
                    </div>
                  ) : null}
                </button>
              ))}

              {sources.length === 0 && (
                <div className="p-3 text-[0.8rem] text-ink-500 italic text-center">
                  No direct sources captured in tracking for this step.
                </div>
              )}
            </div>

            {hasOverflow ? (
              <div className="mt-3 flex justify-end">
                <button
                  type="button"
                  onClick={() => toggleStepExpansion(stepKey)}
                  className="inline-flex items-center gap-1.5 rounded-full border border-border-panel/80 bg-[var(--color-chat-citation-chip-bg)] px-3 py-1.5 text-[0.75rem] font-bold text-ink-500 transition hover:border-border-highlight-soft hover:bg-[var(--color-chat-citation-chip-hover)] hover:text-accent-700"
                >
                  <ChevronDown
                    className={`size-3.5 transition-transform ${isExpanded ? 'rotate-180' : ''}`}
                    strokeWidth={2.4}
                  />
                  {isExpanded
                    ? 'Show fewer sources'
                    : `Show ${sources.length - DEFAULT_VISIBLE_SOURCE_COUNT} more source${sources.length - DEFAULT_VISIBLE_SOURCE_COUNT === 1 ? '' : 's'}`}
                </button>
              </div>
            ) : null}
            </div>
          );
        })}
      </div>

      <div className="border-t border-border-app bg-[var(--color-chat-footer)] px-5 py-3 text-[0.75rem] text-ink-500">
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
