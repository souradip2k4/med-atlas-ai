import { Suspense, lazy, startTransition, useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ChevronRight, MoonStar, SunMedium } from 'lucide-react';

import { AdvancedFiltersModal } from './components/AdvancedFiltersModal';
import { FacilityDetailsPanel } from './components/FacilityDetailsPanel';
import { ResultsSidebar } from './components/ResultsSidebar';
import { TopSearchBar } from './components/TopSearchBar';
import { ChatFab } from './components/ChatFab';
import { ChatPanel } from './components/ChatPanel';
import { fetchFacilityProfile, fetchMapMetadata, searchFacilities } from './lib/api';
import { buildSearchPayload } from './lib/format';
import { useUIStore } from './store/ui-store';

const MapCanvas = lazy(async () => {
  const module = await import('./components/MapCanvas');
  return { default: module.MapCanvas };
});

function App() {
  const {
    activeDropdown,
    advancedOpen,
    chatOpen,
    filters,
    hoveredFacilityId,
    sidebarOpen,
    selectedFacilityId,
    resetFilters,
    setActiveDropdown,
    setAdvancedOpen,
    setCity,
    setHoveredFacilityId,
    setRegion,
    setSelectedFacilityId,
    toggleSidebar,
    setAdvancedFilter,
    clearSpecialties,
    resetAdvancedFilters,
    themePreference,
    toggleAffiliation,
    toggleTheme,
    toggleSpecialty,
    agentMarkers,
    extractedMapMarkers,
  } = useUIStore();
  const [resolvedTheme, setResolvedTheme] = useState<'light' | 'dark'>('light');

  useEffect(() => {
    const media = window.matchMedia('(prefers-color-scheme: dark)');

    const applyTheme = () => {
      const nextTheme =
        themePreference === 'system' ? (media.matches ? 'dark' : 'light') : themePreference;
      setResolvedTheme(nextTheme);
      document.documentElement.dataset.theme = nextTheme;
      document.documentElement.style.colorScheme = nextTheme;
    };

    applyTheme();
    media.addEventListener('change', applyTheme);

    return () => {
      media.removeEventListener('change', applyTheme);
    };
  }, [themePreference]);

  const metadataQuery = useQuery({
    queryKey: ['map-metadata'],
    queryFn: fetchMapMetadata,
    staleTime: 1000 * 60 * 30,
  });

  const searchPayload = buildSearchPayload(filters, null);

  const searchQuery = useQuery({
    queryKey: ['map-search', searchPayload],
    queryFn: () => searchFacilities(searchPayload),
    enabled: Boolean(filters.region),
    staleTime: 1000 * 20,
  });

  const facilityQuery = useQuery({
    queryKey: ['facility-profile', selectedFacilityId],
    queryFn: () => fetchFacilityProfile(selectedFacilityId as string),
    enabled: Boolean(selectedFacilityId),
  });

  const facilities = searchQuery.data?.facilities ?? [];
  const resultCount = searchQuery.data?.count ?? 0;
  const selectedFacilityPreview =
    facilities.find((facility) => facility.facility_id === selectedFacilityId) ?? null;

  return (
    <div
      className="relative flex h-dvh overflow-hidden bg-[radial-gradient(circle_at_72%_18%,var(--color-app-glow-a),transparent_28%),radial-gradient(circle_at_80%_78%,var(--color-app-glow-b),transparent_24%),linear-gradient(180deg,var(--color-surface-app)_0%,var(--color-app-gradient-mid)_44%,var(--color-app-gradient-end)_100%)] max-[920px]:block"
    >
      <div className="pointer-events-none fixed inset-0 bg-[radial-gradient(circle_at_70%_12%,var(--color-overlay-glow-a),transparent_24%),radial-gradient(circle_at_88%_72%,var(--color-overlay-glow-b),transparent_20%)]" />

      <div
        className={`relative shrink-0 overflow-visible transition-[width,opacity,transform] duration-300 ease-out max-[920px]:contents ${
          sidebarOpen
            ? 'w-[340px]'
            : 'pointer-events-none w-0 opacity-100'
        }`}
      >
        <ResultsSidebar
          filters={filters}
          facilities={facilities}
          count={resultCount}
          isLoading={searchQuery.isLoading || searchQuery.isFetching}
          isError={searchQuery.isError}
          errorMessage={
            searchQuery.error instanceof Error
              ? searchQuery.error.message
              : 'Search failed for the current map view.'
          }
          advancedOpen={advancedOpen}
          sidebarOpen={sidebarOpen}
          selectedFacilityId={selectedFacilityId}
          hoveredFacilityId={hoveredFacilityId}
          onAdvancedToggle={() => setAdvancedOpen(!advancedOpen)}
          onFacilitySelect={(facilityId) => {
            startTransition(() => {
              setSelectedFacilityId(facilityId);
            });
          }}
          onFacilityHover={setHoveredFacilityId}
          onClearSearch={resetFilters}
        />

        {sidebarOpen ? (
          <button
            type="button"
            className="absolute left-full top-1/2 z-[32] hidden h-14 w-5 -translate-y-1/2 -ml-px items-center justify-center rounded-r-xl border border-border-white-soft bg-surface-panel-strong text-accent-600 shadow-overlay backdrop-blur-[12px] transition hover:bg-surface-card-strong min-[921px]:inline-flex"
            onClick={toggleSidebar}
            aria-label="Collapse results sidebar"
          >
            <ChevronRight className="size-5 rotate-180" />
          </button>
        ) : null}
      </div>

      <main className="relative min-w-0 flex-1 overflow-hidden">
        {!sidebarOpen ? (
          <button
            type="button"
            className="absolute left-4 top-1/2 z-[35] inline-flex h-14 w-7 -translate-y-1/2 items-center justify-center rounded-r-xl border border-border-white-soft bg-surface-panel-strong text-accent-600 shadow-overlay backdrop-blur-[12px] transition hover:bg-surface-card-strong max-[920px]:hidden"
            onClick={toggleSidebar}
            aria-label="Open results sidebar"
          >
            <ChevronRight className="size-5" />
          </button>
        ) : null}

        <TopSearchBar
          metadata={metadataQuery.data}
          filters={filters}
          resolvedTheme={resolvedTheme}
          activeDropdown={activeDropdown}
          isFacilitySearchLoading={facilityQuery.isLoading || facilityQuery.isFetching}
          onDropdownChange={setActiveDropdown}
          onThemeToggle={() => toggleTheme(resolvedTheme)}
          onFacilitySearch={(facilityName) => {
            startTransition(() => {
              setSelectedFacilityId(facilityName.trim());
            });
          }}
          onRegionSelect={(region) => {
            startTransition(() => {
              setRegion(region);
            });
          }}
          onCitySelect={(city) => {
            startTransition(() => {
              setCity(city);
            });
          }}
          onToggleSpecialty={toggleSpecialty}
          onClearRegion={resetFilters}
          onClearCity={() => setCity('')}
          onClearSpecialties={clearSpecialties}
        />

        <Suspense
          fallback={
            <section className="relative h-dvh w-full overflow-hidden">
              <div className="absolute inset-0 grid gap-2 rounded-panel bg-surface-panel p-6 text-ink-600 shadow-inset-soft">
                <strong className="text-ink-900">Preparing the Ghana map</strong>
                <p>Loading the interactive map workspace and the current healthcare overlays.</p>
              </div>
            </section>
          }
        >
          <MapCanvas
            key={`map-${resolvedTheme}`}
            filters={filters}
            facilities={facilities}
            sidebarOpen={sidebarOpen}
            chatOpen={chatOpen}
            theme={resolvedTheme}
            selectedFacilityPreview={selectedFacilityPreview}
            selectedFacility={facilityQuery.data}
            isFacilityLoading={facilityQuery.isLoading || facilityQuery.isFetching}
            selectedFacilityId={selectedFacilityId}
            hoveredFacilityId={hoveredFacilityId}
            agentMarkers={agentMarkers}
            extractedMapMarkers={extractedMapMarkers}
            onViewportChange={() => {}}
            onFacilitySelect={(facilityId) => {
              startTransition(() => {
                setSelectedFacilityId(facilityId);
              });
            }}
          />
        </Suspense>

        <FacilityDetailsPanel
          profile={facilityQuery.data}
          preview={selectedFacilityPreview}
          selectedFacilityId={selectedFacilityId}
          isLoading={facilityQuery.isLoading || facilityQuery.isFetching}
          isError={facilityQuery.isError}
          errorMessage={
            facilityQuery.error instanceof Error
              ? facilityQuery.error.message
              : 'The facility profile could not be loaded.'
          }
          onClose={() => setSelectedFacilityId(null)}
        />

        <AdvancedFiltersModal
          open={advancedOpen}
          metadata={metadataQuery.data}
          filters={filters}
          onClose={() => setAdvancedOpen(false)}
          onAdvancedFilterChange={setAdvancedFilter}
          onAffiliationToggle={toggleAffiliation}
          onResetAdvancedFilters={resetAdvancedFilters}
        />

        <ChatFab />

        <button
          type="button"
          className="absolute right-4 top-4 z-[34] inline-flex size-11 items-center justify-center rounded-full border border-border-white-soft bg-surface-panel-strong text-accent-600 shadow-overlay backdrop-blur-[14px] transition hover:bg-surface-card-strong min-[921px]:hidden"
          onClick={() => toggleTheme(resolvedTheme)}
          aria-label={`Switch to ${resolvedTheme === 'dark' ? 'light' : 'dark'} mode`}
        >
          {resolvedTheme === 'dark' ? (
            <SunMedium className="size-5" strokeWidth={2.1} />
          ) : (
            <MoonStar className="size-5" strokeWidth={2.1} />
          )}
        </button>
      </main>

      {chatOpen ? <ChatPanel /> : null}
    </div>
  );
}

export default App;
