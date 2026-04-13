import { useRef, useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  BotMessageSquare,
  SendHorizontal,
  RotateCcw,
  Sparkles,
  Square,
  X,
} from 'lucide-react';
import { useUIStore } from '../store/ui-store';
import { invokeAgent } from '../lib/api';
import { ChatCitationsView } from './ChatCitationsView';
import type { AgentResponse } from '../lib/types';

const SUGGESTED_PROMPTS = [
  'Show anomalies in Northern region',
  'Which facilities have MRI equipment?',
  'Find clinics within 20km of Accra',
];

export function ChatPanel() {
  const {
    chatOpen,
    chatEntries,
    viewingCitationsId,
    toggleChat,
    addChatEntry,
    updateChatEntry,
    clearChat,
    setViewingCitationsId,
    setAgentMarkers,
  } = useUIStore();

  const [inputVal, setInputVal] = useState('');
  const endOfMessagesRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const activeRequestIdRef = useRef<string | null>(null);
  const stoppedRequestIdsRef = useRef<Set<string>>(new Set());

  const isAnyLoading = chatEntries.some((e) => e.isLoading);
  const viewedCitations = chatEntries.find((e) => e.id === viewingCitationsId)?.citations;

  useEffect(() => {
    if (chatOpen && !viewingCitationsId) {
      endOfMessagesRef.current?.scrollIntoView({ behavior: 'smooth' });
      // Small delay to allow panel animation to complete
      setTimeout(() => inputRef.current?.focus(), 350);
    }
  }, [chatEntries.length, chatOpen, viewingCitationsId]);

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
      });
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
      isError: false,
      errorMessage: null,
    });
    setAgentMarkers([]);
    activeRequestIdRef.current = null;
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(inputVal);
    }
  };

  if (!chatOpen) return null;

  return (
    <aside className="absolute inset-x-3 bottom-3 top-3 z-[40] flex min-w-0 flex-col overflow-hidden rounded-[24px] border border-border-white-strong bg-surface-panel-strong/98 shadow-panel-strong backdrop-blur-[16px] animate-chat-in min-[921px]:relative min-[921px]:inset-auto min-[921px]:z-20 min-[921px]:h-dvh min-[921px]:w-[45vw] min-[921px]:min-w-[480px] min-[921px]:max-w-[720px] min-[921px]:shrink-0 min-[921px]:rounded-none min-[921px]:border-y-0 min-[921px]:border-r-0 min-[921px]:border-l min-[921px]:border-border-app min-[921px]:shadow-[-18px_0_40px_rgba(19,42,73,0.08)]">
      {viewingCitationsId && viewedCitations ? (
        <ChatCitationsView 
          citations={viewedCitations} 
          onClose={() => setViewingCitationsId(null)} 
        />
      ) : (
        <>
          <div className="flex shrink-0 items-center justify-between border-b border-border-app px-6 py-4">
            <div className="flex items-center gap-3">
              <div className="relative flex size-11 items-center justify-center rounded-full bg-[radial-gradient(circle_at_30%_30%,rgba(79,141,247,0.2),rgba(79,141,247,0.06)_58%,transparent_72%)] text-accent-700">
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

          <div className="flex-1 overflow-y-auto px-6 py-5 scrollbar-thin">
            {chatEntries.length === 0 ? (
              <div className="flex h-full flex-col items-center justify-center text-center opacity-80">
                <div className="relative mb-5 flex size-20 items-center justify-center">
                  <span className="absolute inset-0 rounded-full bg-[radial-gradient(circle_at_30%_30%,rgba(79,141,247,0.2),rgba(79,141,247,0.04)_68%,transparent_72%)] animate-pulse" />
                  <span className="absolute inset-[10px] rounded-full border border-accent-100/90 animate-[spin_12s_linear_infinite]" />
                  <span className="absolute inset-[18px] rounded-full bg-white/80 shadow-[0_12px_28px_rgba(79,141,247,0.12)]" />
                  <Sparkles className="relative z-[1] size-7 text-accent-700" strokeWidth={2} />
                </div>
                <h3 className="mb-2 text-[1.55rem] font-semibold tracking-[-0.03em] text-ink-900">
                  Ask the healthcare assistant
                </h3>
                <p className="mb-8 max-w-[360px] text-[0.98rem] leading-7 text-ink-500">
                  Search equipment, trace service gaps, compare facilities, and validate claims against the Ghana medical dataset.
                </p>
                <div className="flex w-full flex-col gap-2">
                  {SUGGESTED_PROMPTS.map((prompt) => (
                    <button
                      key={prompt}
                      type="button"
                      disabled={isAnyLoading}
                      onClick={() => handleSubmit(prompt)}
                      className="rounded-xl border border-border-app bg-white/60 p-3 text-left text-[0.85rem] font-medium text-ink-700 transition hover:border-border-highlight hover:bg-surface-accent-soft disabled:opacity-50"
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
                    <div className="self-end rounded-[20px] rounded-br-[4px] bg-accent-600 px-4 py-2.5 text-[0.95rem] text-white shadow-sm max-w-[85%]">
                      {entry.userMessage}
                    </div>

                    <div className="flex flex-col items-start gap-1.5 max-w-[92%]">
                      {entry.isLoading && (
                        <div className="inline-flex h-[42px] items-center gap-1.5 rounded-[20px] rounded-bl-[4px] bg-surface-panel px-4 py-3 border border-border-app shadow-sm">
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
                        <div className="prose-chat prose max-w-none rounded-[20px] rounded-bl-[4px] bg-white px-4.5 py-4 border border-border-panel shadow-[0_4px_12px_rgba(25,47,77,0.03)] text-ink-800">
                          <ReactMarkdown remarkPlugins={[remarkGfm]}>
                            {entry.assistantMessage}
                          </ReactMarkdown>
                        </div>
                      )}

                      {entry.citations && entry.citations.summary.total_sources > 0 && (
                        <button
                          type="button"
                          onClick={() => setViewingCitationsId(entry.id)}
                          className="ml-1 mt-0.5 inline-flex items-center gap-1.5 rounded-full bg-surface-filter px-3 py-1.5 text-[0.75rem] font-bold text-ink-500 transition hover:bg-surface-accent-soft hover:text-accent-700"
                        >
                          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M4 22h14a2 2 0 0 0 2-2V7l-5-5H6a2 2 0 0 0-2 2v4"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M3 15h6"/><path d="M3 19h6"/><path d="M10 12h.01"/></svg>
                          View {entry.citations.summary.total_sources} source{entry.citations.summary.total_sources > 1 && 's'}
                        </button>
                      )}
                    </div>
                  </div>
                ))}
                <div ref={endOfMessagesRef} />
              </div>
            )}
          </div>

          <div className="shrink-0 border-t border-border-app bg-white/60 p-5">
            <div className="relative flex items-end">
              <input
                ref={inputRef}
                value={inputVal}
                onChange={(e) => setInputVal(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask about Ghana's medical facilities..."
                disabled={isAnyLoading}
                className="w-full rounded-pill border border-border-field bg-white py-3 pl-4 pr-12 text-[0.95rem] text-ink-900 shadow-inset-field placeholder:text-ink-400 focus:border-accent-400 focus:ring-4 focus:ring-accent-100 disabled:bg-surface-empty disabled:opacity-70 transition-shadow"
              />
              <button
                type="button"
                disabled={isAnyLoading ? false : !inputVal.trim()}
                onClick={isAnyLoading ? handleStop : () => handleSubmit(inputVal)}
                className="absolute bottom-1 right-1 flex size-[38px] items-center justify-center rounded-full bg-accent-600 text-white transition hover:bg-accent-700 disabled:bg-ink-300 disabled:opacity-50"
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
