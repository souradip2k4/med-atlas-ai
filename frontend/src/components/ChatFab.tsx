import { MessageSquareText, X } from 'lucide-react';

import { useUIStore } from '../store/ui-store';

export function ChatFab() {
  const { chatOpen, chatEntries, toggleChat } = useUIStore();
  
  const hasUnread = !chatOpen && chatEntries.length > 0;

  return (
    <button
      type="button"
      className="absolute bottom-8 right-7 z-[15] flex h-14 w-14 items-center justify-center rounded-full bg-white/90 text-accent-700 shadow-overlay backdrop-blur-[12px] transition-all hover:scale-105 hover:bg-white min-[921px]:flex"
      onClick={toggleChat}
      aria-label={chatOpen ? 'Close chat' : 'Open Med-Atlas AI chat'}
    >
      {chatOpen ? (
        <X className="size-6" strokeWidth={2} />
      ) : (
        <MessageSquareText className="size-6" strokeWidth={1.75} />
      )}
      
      {hasUnread && (
        <span className="absolute right-3.5 top-3.5 size-2.5 rounded-full border-2 border-white bg-tone-gold" />
      )}
    </button>
  );
}
