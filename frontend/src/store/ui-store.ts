import { create } from 'zustand';
import { persist } from 'zustand/middleware';

import type { BoundingBox, DropdownKey, SearchFilters } from '../lib/types';

const DEFAULT_FILTERS: SearchFilters = {
  region: '',
  city: '',
  specialties: [],
  facilityType: '',
  operatorType: '',
  organizationType: '',
  affiliationTypes: [],
};

interface UIState {
  activeDropdown: DropdownKey;
  advancedOpen: boolean;
  sidebarOpen: boolean;
  hoveredFacilityId: string | null;
  selectedFacilityId: string | null;
  viewportBbox: BoundingBox | null;
  filters: SearchFilters;
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
      }),
    },
  ),
);
