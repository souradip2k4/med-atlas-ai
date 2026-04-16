import { useRef, useEffect, useState, type PointerEvent as ReactPointerEvent } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  BotMessageSquare,
  MapPinned,
  SendHorizontal,
  RotateCcw,
  Sparkles,
  Square,
  X,
} from 'lucide-react';
import { useUIStore } from '../store/ui-store';
import { extractMapMarkers, invokeAgent } from '../lib/api';
import { ChatCitationsView } from './ChatCitationsView';
import { ChatMappedFacilitiesView } from './ChatMappedFacilitiesView';
import type { AgentResponse, ExtractedMapMarker } from '../lib/types';

const SUGGESTED_PROMPTS = [
  'Show anomalies in Northern region',
  'Which facilities have MRI equipment?',
  'Find clinics within 20km of Accra',
];

const MIN_CHAT_WIDTH_VW = 20;
const MAX_CHAT_WIDTH_VW = 55;

export function ChatPanel() {
  const {
    chatOpen,
    chatEntries,
    viewingCitationsId,
    viewingMappedFacilitiesId,
    toggleChat,
    addChatEntry,
    updateChatEntry,
    clearChat,
    setViewingCitationsId,
    setViewingMappedFacilitiesId,
    setAgentMarkers,
    setExtractedMapMarkers,
  } = useUIStore();

  const [inputVal, setInputVal] = useState('');
  const endOfMessagesRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const isResizingRef = useRef(false);
  const activeRequestIdRef = useRef<string | null>(null);
  const stoppedRequestIdsRef = useRef<Set<string>>(new Set());
  const [desktopWidthVw, setDesktopWidthVw] = useState(MAX_CHAT_WIDTH_VW);

  const isAnyLoading = chatEntries.some((e) => e.isLoading);
  const viewedCitations = chatEntries.find((e) => e.id === viewingCitationsId)?.citations;
  const viewedMappedFacilities =
    chatEntries.find((e) => e.id === viewingMappedFacilitiesId)?.extractedMapMarkers ?? [];

  useEffect(() => {
    if (chatOpen && !viewingCitationsId) {
      endOfMessagesRef.current?.scrollIntoView({ behavior: 'smooth' });
      // Small delay to allow panel animation to complete
      setTimeout(() => inputRef.current?.focus(), 350);
    }
  }, [chatEntries.length, chatOpen, viewingCitationsId]);

  useEffect(() => {
    const handlePointerMove = (event: PointerEvent) => {
      if (!isResizingRef.current || window.innerWidth < 921) {
        return;
      }

      const nextWidth = ((window.innerWidth - event.clientX) / window.innerWidth) * 100;
      const clampedWidth = Math.min(MAX_CHAT_WIDTH_VW, Math.max(MIN_CHAT_WIDTH_VW, nextWidth));
      setDesktopWidthVw(clampedWidth);
    };

    const stopResize = () => {
      if (!isResizingRef.current) {
        return;
      }

      isResizingRef.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    window.addEventListener('pointermove', handlePointerMove);
    window.addEventListener('pointerup', stopResize);
    window.addEventListener('pointercancel', stopResize);
    window.addEventListener('blur', stopResize);

    return () => {
      window.removeEventListener('pointermove', handlePointerMove);
      window.removeEventListener('pointerup', stopResize);
      window.removeEventListener('pointercancel', stopResize);
      window.removeEventListener('blur', stopResize);
      stopResize();
    };
  }, []);

  const handleResizeStart = (event: ReactPointerEvent<HTMLButtonElement>) => {
    if (window.innerWidth < 921) {
      return;
    }

    isResizingRef.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    event.currentTarget.setPointerCapture(event.pointerId);
  };

  const handleSubmit = async (userText: string) => {
    if (!userText.trim() || isAnyLoading) return;

    const entryId = crypto.randomUUID();
    const promptWithContext = userText.trim();
    activeRequestIdRef.current = entryId;

    addChatEntry({
      id: entryId,
      userMessage: userText.trim(),
      assistantMessage: null,
      citations: null,
      isLoading: true,
      isError: false,
      errorMessage: null,
      referencedFacilities: [],
      extractedMapMarkers: [],
      extractedMapMarkersStatus: 'idle',
      extractedMapMarkersError: null,
    });

    setInputVal('');

    try {
      const response: AgentResponse = await invokeAgent(promptWithContext);
      if (stoppedRequestIdsRef.current.has(entryId)) {
        stoppedRequestIdsRef.current.delete(entryId);
        if (activeRequestIdRef.current === entryId) {
          activeRequestIdRef.current = null;
        }
        return;
      }
      
      const assistantMsg = response.output?.find((m) => m.role === 'assistant');
      const text = assistantMsg?.content || 'No response generated.';
      
      const markers: Array<{
        facility_id: string;
        facility_name: string;
        latitude: number;
        longitude: number;
      }> = [];

      // Extract unique coordinates for map sync
      if (response.citations?.steps) {
        response.citations.steps.forEach(step => {
          step.sources?.forEach(source => {
            if (source.facility_id && source.latitude && source.longitude) {
              if (!markers.some(m => m.facility_id === source.facility_id)) {
                markers.push({
                  facility_id: source.facility_id,
                  facility_name: source.facility_name || 'Unknown',
                  latitude: source.latitude,
                  longitude: source.longitude,
                });
              }
            }
          });
        });
      }

      setAgentMarkers(markers);
      
      updateChatEntry(entryId, {
        isLoading: false,
        assistantMessage: text,
        citations: response.citations,
        referencedFacilities: markers,
        extractedMapMarkersStatus: 'loading',
        extractedMapMarkersError: null,
      });

      void (async () => {
        try {
          let extractionResponse = await extractMapMarkers(text);
          if (extractionResponse.map_markers.length === 0) {
            extractionResponse = await extractMapMarkers(text);
          }

          if (stoppedRequestIdsRef.current.has(entryId)) {
            return;
          }

          updateChatEntry(entryId, {
            extractedMapMarkers: extractionResponse.map_markers,
            extractedMapMarkersStatus: 'success',
            extractedMapMarkersError: null,
          });

          const latestEntryId = useUIStore.getState().chatEntries.at(-1)?.id ?? null;
          if (latestEntryId === entryId) {
            setExtractedMapMarkers(entryId, extractionResponse.map_markers);
          }
        } catch (error) {
          updateChatEntry(entryId, {
            extractedMapMarkers: [],
            extractedMapMarkersStatus: 'error',
            extractedMapMarkersError:
              error instanceof Error ? error.message : 'Map extraction failed.',
          });
        }
      })();

      if (activeRequestIdRef.current === entryId) {
        activeRequestIdRef.current = null;
      }
      
    } catch (err) {
      if (stoppedRequestIdsRef.current.has(entryId)) {
        stoppedRequestIdsRef.current.delete(entryId);
        if (activeRequestIdRef.current === entryId) {
          activeRequestIdRef.current = null;
        }
        return;
      }
      console.error('Agent invocation error:', err);
      updateChatEntry(entryId, {
        isLoading: false,
        isError: true,
        errorMessage: err instanceof Error ? err.message : 'An unknown error occurred.',
      });
      if (activeRequestIdRef.current === entryId) {
        activeRequestIdRef.current = null;
      }
    }
  };

  const handleStop = () => {
    const activeId = activeRequestIdRef.current;
    if (!activeId) {
      return;
    }

    stoppedRequestIdsRef.current.add(activeId);
    updateChatEntry(activeId, {
      isLoading: false,
      assistantMessage: null,
      citations: null,
      referencedFacilities: [],
      extractedMapMarkers: [],
      extractedMapMarkersStatus: 'idle',
      extractedMapMarkersError: null,
      isError: false,
      errorMessage: null,
    });
    setAgentMarkers([]);
    setExtractedMapMarkers(null, []);
    activeRequestIdRef.current = null;
  };

  const activateExtractedMarkers = (entryId: string, markers: ExtractedMapMarker[]) => {
    setExtractedMapMarkers(entryId, markers);
    setViewingMappedFacilitiesId(entryId);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(inputVal);
    }
  };

  if (!chatOpen) return null;

  return (
    <aside
      className="absolute inset-x-3 bottom-3 top-3 z-[40] flex min-w-0 flex-col overflow-hidden rounded-[24px] border border-border-white-strong bg-[var(--color-chat-shell)] shadow-panel-strong backdrop-blur-[16px] animate-chat-in min-[921px]:relative min-[921px]:inset-auto min-[921px]:z-20 min-[921px]:h-dvh min-[921px]:shrink-0 min-[921px]:rounded-none min-[921px]:border-y-0 min-[921px]:border-r-0 min-[921px]:border-l min-[921px]:border-border-app min-[921px]:shadow-[-20px_0_48px_rgba(19,42,73,0.1)]"
      style={{
        width: window.innerWidth >= 921 ? `${desktopWidthVw}vw` : undefined,
      }}
    >
      <button
        type="button"
        onPointerDown={handleResizeStart}
        className="absolute inset-y-0 left-0 z-[2] hidden w-3 -translate-x-1/2 cursor-col-resize border-0 bg-transparent min-[921px]:block"
        aria-label="Resize chat panel"
      >
        <span className="absolute left-1/2 top-1/2 h-24 w-1.5 -translate-x-1/2 -translate-y-1/2 rounded-full bg-surface-card/35 opacity-0 shadow-[0_0_0_1px_rgba(255,255,255,0.08)] transition hover:opacity-100 focus:opacity-100" />
      </button>
      {viewingMappedFacilitiesId && viewedMappedFacilities.length > 0 ? (
        <ChatMappedFacilitiesView
          entryId={viewingMappedFacilitiesId}
          facilities={viewedMappedFacilities}
          onClose={() => setViewingMappedFacilitiesId(null)}
        />
      ) : viewingCitationsId && viewedCitations ? (
        <ChatCitationsView 
          citations={viewedCitations} 
          onClose={() => setViewingCitationsId(null)} 
        />
      ) : (
        <>
          <div className="flex shrink-0 items-center justify-between border-b border-border-app/80 bg-[var(--color-chat-header)] px-6 py-2.5 backdrop-blur-[10px]">
            <div className="flex items-center gap-3">
              <div className="relative flex size-11 items-center justify-center rounded-full bg-[var(--color-chat-icon-bg)] text-accent-700">
                <span className="absolute inset-1 rounded-full border border-accent-100/90 animate-pulse" />
                <BotMessageSquare className="relative z-[1] size-5" strokeWidth={2.2} />
              </div>
              <div>
                <div className="text-[0.74rem] font-bold uppercase tracking-[0.2em] text-accent-700">
                  Assistant
                </div>
                <div className="text-sm text-ink-500">Med-Atlas AI copilot</div>
              </div>
            </div>
            <div className="flex items-center gap-1.5">
              {chatEntries.length > 0 && (
                <button
                  type="button"
                  onClick={() => {
                    clearChat();
                    setAgentMarkers([]);
                    setExtractedMapMarkers(null, []);
                  }}
                  className="rounded-full px-3 py-1.5 text-[0.8rem] font-semibold text-ink-500 transition hover:bg-surface-accent hover:text-accent-700"
                >
                  Clear
                </button>
              )}
              <button
                type="button"
                onClick={toggleChat}
                className="flex size-8 items-center justify-center rounded-full text-ink-500 transition hover:bg-surface-accent hover:text-accent-700"
                aria-label="Close chat"
              >
                <X className="size-5" />
              </button>
            </div>
          </div>

          <div className="flex-1 overflow-y-auto bg-[var(--color-chat-body)] px-6 py-5 scrollbar-thin">
            {chatEntries.length === 0 ? (
              <div className="flex h-full flex-col items-center justify-center text-center opacity-80">
                <div className="relative mb-5 flex size-20 items-center justify-center">
                  <span className="absolute inset-0 rounded-full bg-[var(--color-chat-orb-glow)] animate-pulse" />
                  <span className="absolute inset-[10px] rounded-full border border-accent-100/90 animate-[spin_12s_linear_infinite]" />
                  <span className="absolute inset-[18px] rounded-full bg-[var(--color-chat-orb-core)] shadow-[0_18px_38px_rgba(79,141,247,0.16)]" />
                  <Sparkles className="relative z-[1] size-7 text-accent-700" strokeWidth={2} />
                </div>
                <h3 className="mb-2 text-[1.55rem] font-semibold tracking-[-0.03em] text-ink-900">
                  Ask the healthcare assistant
                </h3>
                <p className="mb-8 max-w-[360px] text-[0.98rem] leading-7 text-ink-7">
                  Search equipment, trace service gaps, compare facilities, and validate claims against the Ghana medical dataset.
                </p>
                <div className="flex w-full flex-col gap-2">
                  {SUGGESTED_PROMPTS.map((prompt) => (
                    <button
                      key={prompt}
                      type="button"
                      disabled={isAnyLoading}
                      onClick={() => handleSubmit(prompt)}
                      className="rounded-xl border border-border-app/80 bg-surface-panel-soft p-3 text-left text-[0.85rem] font-medium text-ink-900 shadow-[0_10px_24px_rgba(79,141,247,0.05)] transition hover:border-border-highlight hover:bg-surface-panel-soft disabled:opacity-50"
                    >
                      {prompt}
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              <div className="flex flex-col gap-5">
                {chatEntries.map((entry) => (
                  <div key={entry.id} className="flex flex-col gap-3">
                    <div className="max-w-[85%] self-end rounded-[20px] rounded-br-[4px] bg-[linear-gradient(180deg,#4a84ef,#2f6fe5)] px-4 py-2.5 text-[0.95rem] text-white shadow-[0_12px_26px_rgba(47,111,229,0.26)]">
                      {entry.userMessage}
                    </div>

                    <div className="flex flex-col items-start gap-1.5 max-w-[92%]">
                      {entry.isLoading && (
                        <div className="inline-flex h-[42px] items-center gap-1.5 rounded-[20px] rounded-bl-[4px] border border-border-app/80 bg-[var(--color-chat-assistant-surface)] px-4 py-3 shadow-[0_10px_24px_rgba(22,60,108,0.06)]">
                          <span className="size-1.5 rounded-full bg-ink-400 animate-[typing-dot_1.4s_infinite]" />
                          <span className="size-1.5 rounded-full bg-ink-400 animate-[typing-dot_1.4s_0.2s_infinite]" />
                          <span className="size-1.5 rounded-full bg-ink-400 animate-[typing-dot_1.4s_0.4s_infinite]" />
                        </div>
                      )}

                      {entry.isError && (
                        <div className="rounded-[20px] rounded-bl-[4px] bg-surface-error px-4 py-3 text-[0.95rem] text-red-700 border border-red-100 shadow-sm">
                          <div className="font-semibold mb-1">Failed to generate response</div>
                          <div className="opacity-80 text-sm mb-3 limit-lines">{entry.errorMessage}</div>
                          <button
                            type="button"
                            onClick={() => updateChatEntry(entry.id, { isError: false, errorMessage: null, isLoading: true })}
                            className="inline-flex items-center gap-1.5 text-sm font-semibold hover:underline"
                          >
                            <RotateCcw className="size-3.5" />
                            Retry
                          </button>
                        </div>
                      )}

                      {entry.assistantMessage && (
                        <div className="prose-chat prose max-w-none rounded-[24px] rounded-bl-[6px] border border-[var(--color-chat-card-border)] bg-surface-panel px-5 py-5 text-ink-800 shadow-[var(--color-chat-card-shadow)]">
                          <ReactMarkdown remarkPlugins={[remarkGfm]}>
                            {entry.assistantMessage}
                          </ReactMarkdown>
                        </div>
                      )}

                      {(entry.citations && entry.citations.summary.total_sources > 0) ||
                      entry.extractedMapMarkersStatus !== 'idle' ? (
                        <div className="ml-1 mt-1 flex flex-wrap items-center gap-2">
                          {entry.citations && entry.citations.summary.total_sources > 0 ? (
                            <div className="tooltip-chip-group">
                              <button
                                type="button"
                                onClick={() => setViewingCitationsId(entry.id)}
                                className="inline-flex items-center gap-1.5 rounded-full border border-border-panel/80 bg-[var(--color-chat-citation-chip-bg)] px-3 py-1.5 text-[0.75rem] font-bold text-ink-500 transition hover:border-border-highlight-soft hover:bg-[var(--color-chat-citation-chip-hover)] hover:text-accent-700"
                              >
                                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M4 22h14a2 2 0 0 0 2-2V7l-5-5H6a2 2 0 0 0-2 2v4"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M3 15h6"/><path d="M3 19h6"/><path d="M10 12h.01"/></svg>
                                Citation Sources {entry.citations.summary.total_sources}
                              </button>
                              <span className="chip-tooltip chip-tooltip--start" role="tooltip">
                                Contains citations of all the tools and actual resources that were referred for generating this answer.
                              </span>
                            </div>
                          ) : null}

                          {entry.extractedMapMarkersStatus === 'loading' ? (
                            <div className="inline-flex items-center gap-1.5 rounded-full border border-border-panel/80 bg-[var(--color-chat-citation-chip-bg)] px-3 py-1.5 text-[0.75rem] font-bold text-ink-500">
                              <span className="size-1.5 rounded-full bg-ink-400 animate-[typing-dot_1.4s_infinite]" />
                              <span className="size-1.5 rounded-full bg-ink-400 animate-[typing-dot_1.4s_0.2s_infinite]" />
                              <span className="size-1.5 rounded-full bg-ink-400 animate-[typing-dot_1.4s_0.4s_infinite]" />
                              Finding facilities on map...
                            </div>
                          ) : null}

                          {entry.extractedMapMarkersStatus === 'success' && entry.extractedMapMarkers.length > 0 ? (
                            <div className="tooltip-chip-group">
                              <button
                                type="button"
                                onClick={() => activateExtractedMarkers(entry.id, entry.extractedMapMarkers)}
                                className="inline-flex items-center gap-1.5 rounded-full border border-border-panel/80 bg-[var(--color-chat-citation-chip-bg)] px-3 py-1.5 text-[0.75rem] font-bold text-ink-500 transition hover:border-border-highlight-soft hover:bg-[var(--color-chat-citation-chip-hover)] hover:text-accent-700"
                              >
                                <MapPinned className="size-3.5" strokeWidth={2.2} />
                                Listed Facilities {entry.extractedMapMarkers.length}
                              </button>
                              <span className="chip-tooltip chip-tooltip--end" role="tooltip">
                                List all the facility names specifically that are listed in the prompt answer.
                              </span>
                            </div>
                          ) : null}

                          {entry.extractedMapMarkersStatus === 'success' && entry.extractedMapMarkers.length === 0 ? (
                            <div className="inline-flex items-center gap-1.5 rounded-full border border-border-panel/80 bg-[var(--color-chat-citation-chip-bg)] px-3 py-1.5 text-[0.75rem] font-bold text-ink-500/80">
                              <MapPinned className="size-3.5" strokeWidth={2.2} />
                              No mapped facilities
                            </div>
                          ) : null}

                          {entry.extractedMapMarkersStatus === 'error' ? (
                            <div
                              className="inline-flex items-center gap-1.5 rounded-full border border-border-panel/80 bg-[var(--color-chat-citation-chip-bg)] px-3 py-1.5 text-[0.75rem] font-bold text-ink-500/80"
                              title={entry.extractedMapMarkersError ?? 'Map extraction unavailable'}
                            >
                              <MapPinned className="size-3.5" strokeWidth={2.2} />
                              Map extraction unavailable
                            </div>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  </div>
                ))}
                <div ref={endOfMessagesRef} />
              </div>
            )}
          </div>

          <div className="shrink-0 border-t border-border-app/80 bg-[var(--color-chat-footer)] p-5 backdrop-blur-[12px]">
            <div className="relative flex items-end rounded-[20px] border border-[var(--color-chat-composer-border)] bg-[var(--color-chat-composer-shell)] shadow-[var(--color-chat-composer-shadow)]">
              <input
                ref={inputRef}
                value={inputVal}
                onChange={(e) => setInputVal(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask about Ghana's medical facilities..."
                disabled={isAnyLoading}
                className="w-full rounded-[15px] border border-[var(--color-chat-input-border)] bg-[var(--color-chat-input-bg)] py-3.5 pl-4 pr-13 text-[0.95rem] text-ink-900 shadow-[inset_0_0_0_1px_var(--color-chat-input-inset)] placeholder:text-ink-400 focus:border-accent-400 focus:ring focus:ring-accent-100 disabled:bg-surface-empty disabled:opacity-70 transition-shadow"
              />
              <button
                type="button"
                disabled={isAnyLoading ? false : !inputVal.trim()}
                onClick={isAnyLoading ? handleStop : () => handleSubmit(inputVal)}
                className="absolute bottom-1.5 right-3 flex size-10 items-center justify-center rounded-full bg-accent-600 text-white shadow-[0_10px_22px_rgba(47,111,229,0.28)] transition hover:bg-accent-700 disabled:bg-ink-400 disabled:opacity-90"
                aria-label={isAnyLoading ? 'Stop response' : 'Send message'}
              >
                {isAnyLoading ? (
                  <Square className="size-4 fill-current" strokeWidth={2.5} />
                ) : (
                  <SendHorizontal className="size-4" strokeWidth={2.5} />
                )}
              </button>
            </div>
          </div>
        </>
      )}
    </aside>
  );
}
