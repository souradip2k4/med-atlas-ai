import { useRef, useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { SendHorizontal, RotateCcw, X } from 'lucide-react';
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
    filters,
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
    let promptWithContext = userText.trim();

    // Map context injection for anomaly/validation queries
    if (filters.region) {
      const cityCtx = filters.city ? `, City=${filters.city}` : '';
      promptWithContext += `\n<MAP_CONTEXT: Region=${filters.region}${cityCtx}>`;
    }

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
      
    } catch (err) {
      console.error('Agent invocation error:', err);
      updateChatEntry(entryId, {
        isLoading: false,
        isError: true,
        errorMessage: err instanceof Error ? err.message : 'An unknown error occurred.',
      });
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(inputVal);
    }
  };

  if (!chatOpen) return null;

  return (
    <div className="absolute bottom-6 right-7 z-[20] flex w-[420px] max-w-[calc(100vw-32px)] flex-col overflow-hidden rounded-[24px] border border-border-white-strong bg-surface-panel-strong/98 shadow-panel-strong backdrop-blur-[16px] max-[920px]:bottom-auto max-[920px]:top-4 max-[920px]:h-[calc(100vh-140px)] min-[921px]:h-[600px] max-h-[72vh] animate-chat-in">
      {viewingCitationsId && viewedCitations ? (
        <ChatCitationsView 
          citations={viewedCitations} 
          onClose={() => setViewingCitationsId(null)} 
        />
      ) : (
        <>
          <div className="flex shrink-0 items-center justify-between border-b border-border-app px-5 py-[14px]">
            <h2 className="text-[0.88rem] font-bold uppercase tracking-[0.16em] text-accent-700">
              Med-Atlas AI
            </h2>
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

          <div className="flex-1 overflow-y-auto px-5 py-4 scrollbar-thin">
            {chatEntries.length === 0 ? (
              <div className="flex h-full flex-col items-center justify-center text-center opacity-80">
                <div className="mb-4 size-16 rounded-full bg-[radial-gradient(circle_at_30%_30%,rgba(63,122,234,0.15),transparent_70%)]" />
                <h3 className="mb-1.5 text-lg font-semibold text-ink-900">How can I help?</h3>
                <p className="mb-8 text-ui text-ink-500 max-w-[260px]">
                  Ask me to find specific equipment, analyze regional gaps, or validate medical claims.
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

          <div className="shrink-0 border-t border-border-app bg-white/60 p-4">
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
                disabled={isAnyLoading || !inputVal.trim()}
                onClick={() => handleSubmit(inputVal)}
                className="absolute bottom-1 right-1 flex size-[38px] items-center justify-center rounded-full bg-accent-600 text-white transition hover:bg-accent-700 disabled:bg-ink-300 disabled:opacity-50"
                aria-label="Send message"
              >
                <SendHorizontal className="size-4" strokeWidth={2.5} />
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
