import { create } from 'zustand';
import { persist } from 'zustand/middleware';

import type {
  BoundingBox,
  ChatEntry,
  DropdownKey,
  ExtractedMapMarker,
  SearchFilters,
  ThemePreference,
} from '../lib/types';

const DEFAULT_FILTERS: SearchFilters = {
  region: '',
  city: '',
  specialties: [],
  facilityType: '',
  operatorType: '',
  organizationType: '',
  affiliationTypes: [],
};

function sanitizePersistedChatEntries(chatEntries: ChatEntry[]) {
  return chatEntries.map((entry) => {
    if (!entry.isLoading && entry.extractedMapMarkersStatus !== 'loading') {
      return entry;
    }

    return {
      ...entry,
      isLoading: false,
      extractedMapMarkersStatus:
        entry.extractedMapMarkersStatus === 'loading' ? 'idle' : entry.extractedMapMarkersStatus,
      extractedMapMarkersError:
        entry.extractedMapMarkersStatus === 'loading' ? null : entry.extractedMapMarkersError,
    };
  });
}

interface UIState {
  activeDropdown: DropdownKey;
  advancedOpen: boolean;
  sidebarOpen: boolean;
  hoveredFacilityId: string | null;
  selectedFacilityId: string | null;
  viewportBbox: BoundingBox | null;
  filters: SearchFilters;
  chatOpen: boolean;
  chatEntries: ChatEntry[];
  viewingCitationsId: string | null;
  viewingMappedFacilitiesId: string | null;
  themePreference: ThemePreference;
  agentMarkers: Array<{
    facility_id: string;
    facility_name: string;
    latitude: number;
    longitude: number;
  }>;
  extractedMapMarkers: ExtractedMapMarker[];
  activeExtractedMapEntryId: string | null;
  setChatOpen: (open: boolean) => void;
  toggleChat: () => void;
  setThemePreference: (theme: ThemePreference) => void;
  toggleTheme: (resolvedTheme: 'light' | 'dark') => void;
  addChatEntry: (entry: ChatEntry) => void;
  updateChatEntry: (id: string, updates: Partial<ChatEntry>) => void;
  setViewingCitationsId: (id: string | null) => void;
  setViewingMappedFacilitiesId: (id: string | null) => void;
  setAgentMarkers: (markers: UIState['agentMarkers']) => void;
  setExtractedMapMarkers: (entryId: string | null, markers: ExtractedMapMarker[]) => void;
  clearChat: () => void;
  setActiveDropdown: (dropdown: DropdownKey) => void;
  setAdvancedOpen: (open: boolean) => void;
  toggleSidebar: () => void;
  setSidebarOpen: (open: boolean) => void;
  setHoveredFacilityId: (facilityId: string | null) => void;
  setSelectedFacilityId: (facilityId: string | null) => void;
  setViewportBbox: (bbox: BoundingBox | null) => void;
  setRegion: (region: string) => void;
  setCity: (city: string) => void;
  toggleSpecialty: (specialty: string) => void;
  clearSpecialties: () => void;
  setAdvancedFilter: (
    key: 'facilityType' | 'operatorType' | 'organizationType',
    value: string,
  ) => void;
  toggleAffiliation: (value: string) => void;
  resetAdvancedFilters: () => void;
  resetFilters: () => void;
}

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      activeDropdown: null,
      advancedOpen: false,
      sidebarOpen: true,
      hoveredFacilityId: null,
      selectedFacilityId: null,
      viewportBbox: null,
      filters: DEFAULT_FILTERS,
      chatOpen: true,
      chatEntries: [],
      viewingCitationsId: null,
      viewingMappedFacilitiesId: null,
      themePreference: 'system',
      agentMarkers: [],
      extractedMapMarkers: [],
      activeExtractedMapEntryId: null,
      setChatOpen: (open) => set({ chatOpen: open }),
      toggleChat: () => set((state) => ({ chatOpen: !state.chatOpen })),
      setThemePreference: (themePreference) => set({ themePreference }),
      toggleTheme: (resolvedTheme) =>
        set({
          themePreference: resolvedTheme === 'dark' ? 'light' : 'dark',
        }),
      addChatEntry: (entry) =>
        set((state) => ({
          chatEntries: [...state.chatEntries, entry],
        })),
      updateChatEntry: (id, updates) =>
        set((state) => ({
          chatEntries: state.chatEntries.map((entry) =>
            entry.id === id ? { ...entry, ...updates } : entry
          ),
        })),
      setViewingCitationsId: (id) => set({ viewingCitationsId: id }),
      setViewingMappedFacilitiesId: (id) => set({ viewingMappedFacilitiesId: id }),
      setAgentMarkers: (markers) => set({ agentMarkers: markers }),
      setExtractedMapMarkers: (entryId, markers) =>
        set({
          activeExtractedMapEntryId: entryId,
          extractedMapMarkers: markers,
        }),
      clearChat: () =>
        set({
          chatEntries: [],
          viewingCitationsId: null,
          viewingMappedFacilitiesId: null,
          agentMarkers: [],
          extractedMapMarkers: [],
          activeExtractedMapEntryId: null,
        }),
      setActiveDropdown: (dropdown) => set({ activeDropdown: dropdown }),
      setAdvancedOpen: (open) => set({ advancedOpen: open }),
      toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      setSidebarOpen: (open) => set({ sidebarOpen: open }),
      setHoveredFacilityId: (facilityId) => set({ hoveredFacilityId: facilityId }),
      setSelectedFacilityId: (facilityId) => set({ selectedFacilityId: facilityId }),
      setViewportBbox: (bbox) => set({ viewportBbox: bbox }),
      setRegion: (region) =>
        set((state) => ({
          filters: {
            ...state.filters,
            region,
            city: '',
          },
          selectedFacilityId: null,
        })),
      setCity: (city) =>
        set((state) => ({
          filters: {
            ...state.filters,
            city,
          },
          selectedFacilityId: null,
        })),
      toggleSpecialty: (specialty) =>
        set((state) => {
          const exists = state.filters.specialties.includes(specialty);
          return {
            filters: {
              ...state.filters,
              specialties: exists
                ? state.filters.specialties.filter((item) => item !== specialty)
                : [...state.filters.specialties, specialty],
            },
            selectedFacilityId: null,
          };
        }),
      clearSpecialties: () =>
        set((state) => ({
          filters: {
            ...state.filters,
            specialties: [],
          },
          selectedFacilityId: null,
        })),
      setAdvancedFilter: (key, value) =>
        set((state) => ({
          filters: {
            ...state.filters,
            [key]: value,
          },
          selectedFacilityId: null,
        })),
      toggleAffiliation: (value) =>
        set((state) => {
          const exists = state.filters.affiliationTypes.includes(value);
          return {
            filters: {
              ...state.filters,
              affiliationTypes: exists
                ? state.filters.affiliationTypes.filter((item) => item !== value)
                : [...state.filters.affiliationTypes, value],
            },
            selectedFacilityId: null,
          };
        }),
      resetAdvancedFilters: () =>
        set((state) => ({
          filters: {
            ...state.filters,
            facilityType: '',
            operatorType: '',
            organizationType: '',
            affiliationTypes: [],
          },
          selectedFacilityId: null,
        })),
      resetFilters: () =>
        set({
          activeDropdown: null,
          advancedOpen: false,
          hoveredFacilityId: null,
          selectedFacilityId: null,
          viewportBbox: null,
          filters: DEFAULT_FILTERS,
        }),
    }),
    {
      name: 'med-atlas-map-ui',
      partialize: (state) => ({
        filters: state.filters,
        themePreference: state.themePreference,
        chatOpen: state.chatOpen,
        chatEntries: state.chatEntries,
        viewingCitationsId: state.viewingCitationsId,
        viewingMappedFacilitiesId: state.viewingMappedFacilitiesId,
        agentMarkers: state.agentMarkers,
        extractedMapMarkers: state.extractedMapMarkers,
        activeExtractedMapEntryId: state.activeExtractedMapEntryId,
      }),
      merge: (persistedState, currentState) => {
        const typedPersistedState = persistedState as Partial<UIState> | undefined;

        if (!typedPersistedState) {
          return currentState;
        }

        return {
          ...currentState,
          ...typedPersistedState,
          chatEntries: sanitizePersistedChatEntries(
            typedPersistedState.chatEntries ?? currentState.chatEntries,
          ),
          viewingCitationsId: typedPersistedState.viewingCitationsId ?? null,
          viewingMappedFacilitiesId: typedPersistedState.viewingMappedFacilitiesId ?? null,
          agentMarkers: typedPersistedState.agentMarkers ?? currentState.agentMarkers,
          extractedMapMarkers:
            typedPersistedState.extractedMapMarkers ?? currentState.extractedMapMarkers,
          activeExtractedMapEntryId:
            typedPersistedState.activeExtractedMapEntryId ?? currentState.activeExtractedMapEntryId,
          chatOpen: typedPersistedState.chatOpen ?? currentState.chatOpen,
        };
      },
    },
  ),
);
