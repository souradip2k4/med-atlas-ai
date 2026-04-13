import { Suspense, lazy, startTransition } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ChevronRight } from 'lucide-react';

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
    toggleAffiliation,
    toggleSpecialty,
    agentMarkers,
  } = useUIStore();

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
      className="relative flex h-dvh overflow-hidden bg-[radial-gradient(circle_at_72%_18%,rgba(140,199,255,0.35),transparent_28%),radial-gradient(circle_at_80%_78%,rgba(27,148,122,0.2),transparent_24%),linear-gradient(180deg,var(--color-surface-app)_0%,#f5f8fc_44%,#e7eef9_100%)] max-[920px]:block"
    >
      <div className="pointer-events-none fixed inset-0 bg-[radial-gradient(circle_at_70%_12%,rgba(55,118,226,0.15),transparent_24%),radial-gradient(circle_at_88%_72%,rgba(8,145,178,0.14),transparent_20%)]" />

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
            className="absolute left-full top-1/2 z-[32] hidden h-14 w-5 -translate-y-1/2 -ml-px items-center justify-center rounded-r-xl border border-white/90 bg-white/94 text-accent-600 shadow-overlay backdrop-blur-[12px] transition hover:bg-white min-[921px]:inline-flex"
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
            className="absolute left-4 top-1/2 z-[35] inline-flex h-14 w-7 -translate-y-1/2 items-center justify-center rounded-r-xl border border-white/90 bg-white/94 text-accent-600 shadow-overlay backdrop-blur-[12px] transition hover:bg-white max-[920px]:hidden"
            onClick={toggleSidebar}
            aria-label="Open results sidebar"
          >
            <ChevronRight className="size-5" />
          </button>
        ) : null}

        <TopSearchBar
          metadata={metadataQuery.data}
          filters={filters}
          activeDropdown={activeDropdown}
          onDropdownChange={setActiveDropdown}
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
            filters={filters}
            facilities={facilities}
            sidebarOpen={sidebarOpen}
            selectedFacilityPreview={selectedFacilityPreview}
            selectedFacility={facilityQuery.data}
            isFacilityLoading={facilityQuery.isLoading || facilityQuery.isFetching}
            selectedFacilityId={selectedFacilityId}
            hoveredFacilityId={hoveredFacilityId}
            agentMarkers={agentMarkers}
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
        <ChatPanel />
      </main>
    </div>
  );
}

export default App;
