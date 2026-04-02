import { Suspense, lazy, startTransition } from 'react';
import { useQuery } from '@tanstack/react-query';

import { FacilityDetailsPanel } from './components/FacilityDetailsPanel';
import { ResultsSidebar } from './components/ResultsSidebar';
import { TopSearchBar } from './components/TopSearchBar';
import { fetchFacilityProfile, fetchMapMetadata, searchFacilities } from './lib/api';
import { buildSearchPayload } from './lib/format';
import { useDebouncedValue } from './lib/hooks';
import { useUIStore } from './store/ui-store';
import './App.css';

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
    selectedFacilityId,
    viewportBbox,
    resetFilters,
    setActiveDropdown,
    setAdvancedOpen,
    setCity,
    setHoveredFacilityId,
    setRegion,
    setSelectedFacilityId,
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
    <div className="app-shell">
      <div className="app-backdrop" />

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
        selectedFacilityId={selectedFacilityId}
        hoveredFacilityId={hoveredFacilityId}
        onAdvancedToggle={() => setAdvancedOpen(!advancedOpen)}
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

      <main className="workspace">
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
            <section className="map-shell">
              <div className="map-fallback">
                <strong>Preparing the Ghana map</strong>
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
