import { Suspense, lazy, startTransition } from 'react';
import { useQuery } from '@tanstack/react-query';
import { PanelLeftOpen } from 'lucide-react';

import { FacilityDetailsPanel } from './components/FacilityDetailsPanel';
import { ResultsSidebar } from './components/ResultsSidebar';
import { TopSearchBar } from './components/TopSearchBar';
import { fetchFacilityProfile, fetchMapMetadata, searchFacilities } from './lib/api';
import { buildSearchPayload } from './lib/format';
import { useDebouncedValue } from './lib/hooks';
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
    viewportBbox,
    resetFilters,
    setActiveDropdown,
    setAdvancedOpen,
    setCity,
    setHoveredFacilityId,
    setRegion,
    setSelectedFacilityId,
    toggleSidebar,
    setViewportBbox,
    setAdvancedFilter,
    clearSpecialties,
    toggleAffiliation,
    toggleSpecialty,
  } = useUIStore();

  const debouncedBbox = useDebouncedValue(viewportBbox, 450);

  const metadataQuery = useQuery({
    queryKey: ['map-metadata'],
    queryFn: fetchMapMetadata,
    staleTime: 1000 * 60 * 30,
  });

  const searchPayload = buildSearchPayload(filters, filters.region ? debouncedBbox : null);

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

  return (
    <div
      className={`relative grid min-h-dvh bg-[radial-gradient(circle_at_72%_18%,rgba(140,199,255,0.35),transparent_28%),radial-gradient(circle_at_80%_78%,rgba(27,148,122,0.2),transparent_24%),linear-gradient(180deg,var(--color-surface-app)_0%,#f5f8fc_44%,#e7eef9_100%)] max-[920px]:block ${
        sidebarOpen
          ? 'grid-cols-[430px_minmax(0,1fr)] max-[1180px]:grid-cols-[360px_minmax(0,1fr)]'
          : 'grid-cols-[0_minmax(0,1fr)]'
      }`}
    >
      <div className="pointer-events-none fixed inset-0 bg-[radial-gradient(circle_at_70%_12%,rgba(55,118,226,0.15),transparent_24%),radial-gradient(circle_at_88%_72%,rgba(8,145,178,0.14),transparent_20%)]" />

      <div
        className={`relative overflow-hidden transition-[width,opacity,transform] duration-300 max-[920px]:contents ${
          sidebarOpen
            ? 'w-[430px] opacity-100 max-[1180px]:w-[360px]'
            : 'w-0 opacity-0 -translate-x-6 pointer-events-none'
        }`}
      >
        <ResultsSidebar
          metadata={metadataQuery.data}
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
          onSidebarToggle={toggleSidebar}
          onAdvancedFilterChange={setAdvancedFilter}
          onAffiliationToggle={toggleAffiliation}
          onFacilitySelect={(facilityId) => {
            startTransition(() => {
              setSelectedFacilityId(facilityId);
            });
          }}
          onFacilityHover={setHoveredFacilityId}
          onClearSearch={resetFilters}
        />
      </div>

      <main className="relative min-h-dvh overflow-hidden">
        {!sidebarOpen ? (
          <button
            type="button"
            className="absolute left-4 top-5 z-[35] inline-flex size-11 items-center justify-center rounded-full border border-white/90 bg-white/92 text-accent-600 shadow-overlay backdrop-blur-[12px] transition hover:bg-white max-[920px]:hidden"
            onClick={toggleSidebar}
            aria-label="Open results sidebar"
          >
            <PanelLeftOpen className="size-5" />
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
            selectedFacility={facilityQuery.data}
            selectedFacilityId={selectedFacilityId}
            hoveredFacilityId={hoveredFacilityId}
            onViewportChange={setViewportBbox}
            onFacilitySelect={(facilityId) => {
              startTransition(() => {
                setSelectedFacilityId(facilityId);
              });
            }}
          />
        </Suspense>

        <FacilityDetailsPanel
          profile={facilityQuery.data}
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
      </main>
    </div>
  );
}

export default App;
