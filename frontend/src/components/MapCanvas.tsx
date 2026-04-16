import { useEffect, useEffectEvent, useRef } from 'react';
import { Search } from 'lucide-react';
import mapboxgl from 'mapbox-gl';

import { geocodePlace, getHighlightFeature } from '../lib/api';
import { GHANA_BOUNDS, GHANA_VIEW, formatLabel } from '../lib/format';
import { MAPBOX_TOKEN } from '../lib/env';
import type {
  BoundingBox,
  ExtractedMapMarker,
  FacilityProfile,
  FacilitySummary,
  SearchFilters,
  ThemeMode,
} from '../lib/types';

interface MapCanvasProps {
  filters: SearchFilters;
  facilities: FacilitySummary[];
  sidebarOpen: boolean;
  chatOpen: boolean;
  theme: ThemeMode;
  selectedFacilityPreview: FacilitySummary | null;
  selectedFacility: FacilityProfile | null | undefined;
  isFacilityLoading: boolean;
  selectedFacilityId: string | null;
  hoveredFacilityId: string | null;
  onViewportChange: (bbox: BoundingBox | null) => void;
  onFacilitySelect: (facilityId: string) => void;
  agentMarkers: Array<{
    facility_id: string;
    facility_name: string;
    latitude: number;
    longitude: number;
  }>;
  extractedMapMarkers: ExtractedMapMarker[];
}

function createMarkerElement(active: boolean, label: string) {
  const element = document.createElement('button');
  element.type = 'button';
  element.className = active
    ? 'facility-marker relative h-7 w-7 border-0 bg-transparent'
    : 'facility-marker relative h-7 w-7 border-0 bg-transparent';
  element.setAttribute('aria-label', label);
  element.innerHTML = active
      ? '<span class="facility-marker__halo absolute inset-0 rounded-full bg-surface-teal"></span><span class="facility-marker__dot absolute inset-[3px] rounded-full border-[3px] border-white/95 bg-[linear-gradient(180deg,#4f8df7,#2d6ce6)] shadow-[0_10px_18px_rgba(15,97,82,0.25)]"></span>'
    : '<span class="facility-marker__halo absolute inset-0 rounded-full bg-surface-teal"></span><span class="facility-marker__dot absolute inset-[5px] rounded-full border-[3px] border-white/95 bg-[linear-gradient(180deg,#22c7a7,#0d9f83)] shadow-[0_10px_18px_rgba(15,97,82,0.25)]"></span>';
  return element;
}

function createAgentMarkerElement(label: string) {
  const element = document.createElement('button');
  element.type = 'button';
  element.className = 'agent-marker relative h-7 w-7 border-0 bg-transparent';
  element.setAttribute('aria-label', label);
  element.innerHTML =
    '<span class="agent-marker__halo absolute inset-0 rounded-full"></span><span class="agent-marker__dot absolute inset-[3px] rounded-full"></span>';
  return element;
}

function createExtractedMarkerElement(active: boolean, label: string) {
  const element = document.createElement('button');
  element.type = 'button';
  element.className = active
    ? 'extracted-marker extracted-marker--active relative h-10 w-10 border-0 bg-transparent'
    : 'extracted-marker relative h-10 w-10 border-0 bg-transparent';
  element.setAttribute('aria-label', label);
  element.innerHTML = `
    <svg viewBox="0 0 24 24" fill="none" class="extracted-marker__icon">
      <path d="M12 21s-5.5-4.7-5.5-10a5.5 5.5 0 1 1 11 0c0 5.3-5.5 10-5.5 10Z" class="extracted-marker__pin"/>
      <circle cx="12" cy="10" r="3.2" class="extracted-marker__inner"/>
      <path d="m10.5 10 1.1 1.1 2-2.2" class="extracted-marker__check"/>
    </svg>
  `;
  return element;
}

function createFocusMarkerElement(label: string) {
  const element = document.createElement('div');
  element.className = 'facility-focus-marker relative h-14 w-14';
  element.setAttribute('aria-label', label);
  element.innerHTML =
    '<span class="facility-focus-marker__pulse absolute inset-0 rounded-full"></span><span class="facility-focus-marker__ring absolute inset-[7px] rounded-full"></span><span class="facility-focus-marker__core absolute inset-[14px] rounded-full"></span>';
  return element;
}

function createLoadingPinElement(label: string) {
  const element = document.createElement('div');
  element.className = 'facility-loading-pin relative h-11 w-11';
  element.setAttribute('aria-label', label);

  const pulse = document.createElement('span');
  pulse.className = 'facility-loading-pin__pulse';

  const pin = document.createElement('span');
  pin.className = 'facility-loading-pin__body';

  const glyph = document.createElement('span');
  glyph.className = 'facility-loading-pin__glyph';
  glyph.textContent = '❤';

  pin.appendChild(glyph);
  element.appendChild(pulse);
  element.appendChild(pin);

  return element;
}

function createLoadingLabelElement(label: string) {
  const element = document.createElement('div');
  element.className = 'facility-loading-label';
  element.textContent = label;
  return element;
}

function getMapBounds(map: mapboxgl.Map): BoundingBox {
  const bounds = map.getBounds();
  if (!bounds) {
    return [GHANA_BOUNDS[0][1], GHANA_BOUNDS[0][0], GHANA_BOUNDS[1][1], GHANA_BOUNDS[1][0]];
  }

  return [
    bounds.getSouth(),
    bounds.getWest(),
    bounds.getNorth(),
    bounds.getEast(),
  ];
}

function toRadians(value: number) {
  return (value * Math.PI) / 180;
}

function haversineDistanceKm(
  first: Pick<ExtractedMapMarker, 'latitude' | 'longitude'>,
  second: Pick<ExtractedMapMarker, 'latitude' | 'longitude'>,
) {
  const earthRadiusKm = 6371;
  const latitudeDelta = toRadians(second.latitude - first.latitude);
  const longitudeDelta = toRadians(second.longitude - first.longitude);
  const firstLatitude = toRadians(first.latitude);
  const secondLatitude = toRadians(second.latitude);

  const a =
    Math.sin(latitudeDelta / 2) ** 2 +
    Math.cos(firstLatitude) * Math.cos(secondLatitude) * Math.sin(longitudeDelta / 2) ** 2;

  return 2 * earthRadiusKm * Math.asin(Math.sqrt(a));
}

function selectPrimaryExtractedCluster(markers: ExtractedMapMarker[]) {
  if (markers.length <= 2) {
    return markers;
  }

  const thresholdKm = 60;
  const visited = new Set<number>();
  const components: ExtractedMapMarker[][] = [];

  for (let index = 0; index < markers.length; index += 1) {
    if (visited.has(index)) {
      continue;
    }

    const queue = [index];
    const component: ExtractedMapMarker[] = [];
    visited.add(index);

    while (queue.length > 0) {
      const currentIndex = queue.shift() as number;
      component.push(markers[currentIndex]);

      for (let nextIndex = 0; nextIndex < markers.length; nextIndex += 1) {
        if (visited.has(nextIndex)) {
          continue;
        }

        if (haversineDistanceKm(markers[currentIndex], markers[nextIndex]) <= thresholdKm) {
          visited.add(nextIndex);
          queue.push(nextIndex);
        }
      }
    }

    components.push(component);
  }

  const largestComponent = [...components].sort((first, second) => second.length - first.length)[0];
  const majoritySize = Math.max(2, Math.ceil(markers.length / 2));

  if (largestComponent && largestComponent.length >= majoritySize) {
    return largestComponent;
  }

  const subsetSize = Math.max(2, Math.ceil(markers.length * 0.6));
  let bestSubset = markers.slice(0, subsetSize);
  let bestRadius = Number.POSITIVE_INFINITY;

  markers.forEach((marker) => {
    const nearest = [...markers]
      .sort((first, second) => haversineDistanceKm(marker, first) - haversineDistanceKm(marker, second))
      .slice(0, subsetSize);

    const radius = nearest.reduce((maximum, candidate) => {
      return Math.max(maximum, haversineDistanceKm(marker, candidate));
    }, 0);

    if (radius < bestRadius) {
      bestRadius = radius;
      bestSubset = nearest;
    }
  });

  return bestSubset;
}

function getExtractedMarkerOffsets(
  map: mapboxgl.Map,
  markers: ExtractedMapMarker[],
): Map<string, [number, number]> {
  const overlapThresholdPx = 22;
  const offsets = new Map<string, [number, number]>();
  const assigned = new Set<number>();
  const projectedPoints = markers.map((marker) => map.project([marker.longitude, marker.latitude]));

  for (let index = 0; index < markers.length; index += 1) {
    if (assigned.has(index)) {
      continue;
    }

    const cluster = [index];
    assigned.add(index);

    for (let nextIndex = index + 1; nextIndex < markers.length; nextIndex += 1) {
      if (assigned.has(nextIndex)) {
        continue;
      }

      const dx = projectedPoints[index].x - projectedPoints[nextIndex].x;
      const dy = projectedPoints[index].y - projectedPoints[nextIndex].y;
      const distance = Math.hypot(dx, dy);

      if (distance <= overlapThresholdPx) {
        cluster.push(nextIndex);
        assigned.add(nextIndex);
      }
    }

    if (cluster.length === 1) {
      offsets.set(markers[index].id, [0, 0]);
      continue;
    }

    const sortedCluster = cluster.sort((first, second) => {
      return markers[first].id.localeCompare(markers[second].id);
    });

    const baseRadius = 18;
    const ringStep = 8;
    const ringCapacity = 8;

    sortedCluster.forEach((markerIndex, clusterIndex) => {
      const ring = Math.floor(clusterIndex / ringCapacity);
      const positionInRing = clusterIndex % ringCapacity;
      const pointsInRing = Math.min(ringCapacity, sortedCluster.length - ring * ringCapacity);
      const angle = (-Math.PI / 2) + (positionInRing / pointsInRing) * Math.PI * 2;
      const radius = baseRadius + ring * ringStep;
      const x = Math.round(Math.cos(angle) * radius);
      const y = Math.round(Math.sin(angle) * radius);
      offsets.set(markers[markerIndex].id, [x, y]);
    });
  }

  return offsets;
}

export function MapCanvas({
  filters,
  facilities,
  sidebarOpen,
  chatOpen,
  theme,
  selectedFacilityPreview,
  selectedFacility,
  isFacilityLoading,
  selectedFacilityId,
  hoveredFacilityId,
  onViewportChange,
  onFacilitySelect,
  agentMarkers,
  extractedMapMarkers,
}: MapCanvasProps) {
  const mapNodeRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<mapboxgl.Map | null>(null);
  const markersRef = useRef<mapboxgl.Marker[]>([]);
  const agentMarkersRef = useRef<mapboxgl.Marker[]>([]);
  const extractedMarkersRef = useRef<mapboxgl.Marker[]>([]);
  const focusMarkerRef = useRef<mapboxgl.Marker | null>(null);
  const loadingPinMarkerRef = useRef<mapboxgl.Marker | null>(null);
  const loadingLabelMarkerRef = useRef<mapboxgl.Marker | null>(null);

  const syncViewport = useEffectEvent(() => {
    if (mapRef.current) {
      onViewportChange(getMapBounds(mapRef.current));
    }
  });

  useEffect(() => {
    if (!mapNodeRef.current || mapRef.current || !MAPBOX_TOKEN) {
      return;
    }

    mapboxgl.accessToken = MAPBOX_TOKEN;

    const map = new mapboxgl.Map({
      container: mapNodeRef.current,
      style:
        theme === 'dark'
          ? 'mapbox://styles/mapbox/dark-v11'
          : 'mapbox://styles/mapbox/streets-v12',
      center: GHANA_VIEW.center,
      zoom: GHANA_VIEW.zoom,
      maxBounds: GHANA_BOUNDS,
      attributionControl: false,
      dragRotate: false,
      touchZoomRotate: false,
      pitchWithRotate: false,
    });

    map.addControl(
      new mapboxgl.NavigationControl({
        showCompass: false,
      }),
      'bottom-right',
    );

    map.on('load', () => {
      map.addSource('selection-focus', {
        type: 'geojson',
        data: {
          type: 'FeatureCollection',
          features: [],
        },
      });

      map.addLayer({
        id: 'selection-fill',
        type: 'fill',
        source: 'selection-focus',
        paint: {
          'fill-color': '#4f8df7',
          'fill-opacity': 0.14,
        },
        filter: ['==', ['geometry-type'], 'Polygon'],
      });

      map.addLayer({
        id: 'selection-outline',
        type: 'line',
        source: 'selection-focus',
        paint: {
          'line-color': '#2d6ce6',
          'line-width': 3,
          'line-opacity': 0.88,
        },
        filter: ['==', ['geometry-type'], 'Polygon'],
      });

      syncViewport();
    });

    map.on('moveend', syncViewport);
    mapRef.current = map;

    const firstResizeId = window.requestAnimationFrame(() => {
      map.resize();
    });

    const secondResizeId = window.setTimeout(() => {
      map.resize();
      syncViewport();
    }, 180);

    const handleWindowResize = () => {
      map.resize();
      syncViewport();
    };

    window.addEventListener('resize', handleWindowResize);

    const resizeObserver =
      typeof ResizeObserver !== 'undefined'
        ? new ResizeObserver(() => {
            map.resize();
            syncViewport();
          })
        : null;

    if (resizeObserver && mapNodeRef.current) {
      resizeObserver.observe(mapNodeRef.current);
    }

    return () => {
      window.cancelAnimationFrame(firstResizeId);
      window.clearTimeout(secondResizeId);
      window.removeEventListener('resize', handleWindowResize);
      resizeObserver?.disconnect();
      markersRef.current.forEach((marker) => marker.remove());
      markersRef.current = [];
      agentMarkersRef.current.forEach((marker) => marker.remove());
      agentMarkersRef.current = [];
      extractedMarkersRef.current.forEach((marker) => marker.remove());
      extractedMarkersRef.current = [];
      focusMarkerRef.current?.remove();
      focusMarkerRef.current = null;
      loadingPinMarkerRef.current?.remove();
      loadingPinMarkerRef.current = null;
      loadingLabelMarkerRef.current?.remove();
      loadingLabelMarkerRef.current = null;
      map.remove();
      mapRef.current = null;
    };
  }, [theme]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }

    markersRef.current.forEach((marker) => marker.remove());
    markersRef.current = [];

    facilities
      .filter((facility) => facility.latitude !== null && facility.longitude !== null)
      .forEach((facility) => {
        const active =
          facility.facility_id === selectedFacilityId ||
          facility.facility_id === hoveredFacilityId;

        const element = createMarkerElement(
          active,
          `${facility.facility_name} in ${facility.city ?? 'Ghana'}`,
        );

        element.addEventListener('click', () => {
          onFacilitySelect(facility.facility_id);
        });

        const marker = new mapboxgl.Marker({
          element,
          anchor: 'center',
        })
          .setLngLat([facility.longitude as number, facility.latitude as number])
          .addTo(map);

        markersRef.current.push(marker);
      });
  }, [facilities, hoveredFacilityId, onFacilitySelect, selectedFacilityId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }

    agentMarkersRef.current.forEach((marker) => marker.remove());
    agentMarkersRef.current = [];

    if (!agentMarkers || agentMarkers.length === 0) {
      return;
    }

    agentMarkers.forEach((facility) => {
      const element = createAgentMarkerElement(facility.facility_name);

      element.addEventListener('click', () => {
        onFacilitySelect(facility.facility_id);
      });

      const marker = new mapboxgl.Marker({
        element,
        anchor: 'center',
      })
        .setLngLat([facility.longitude, facility.latitude])
        .addTo(map);

      agentMarkersRef.current.push(marker);
    });
  }, [agentMarkers, onFacilitySelect]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }

    const renderExtractedMarkers = () => {
      extractedMarkersRef.current.forEach((marker) => marker.remove());
      extractedMarkersRef.current = [];

      if (!extractedMapMarkers || extractedMapMarkers.length === 0) {
        return;
      }

      const offsets = getExtractedMarkerOffsets(map, extractedMapMarkers);

      extractedMapMarkers.forEach((facility) => {
        const element = createExtractedMarkerElement(
          facility.id === selectedFacilityId,
          `${facility.name} extracted from response`,
        );

        element.addEventListener('click', () => {
          onFacilitySelect(facility.id);
        });

        const marker = new mapboxgl.Marker({
          element,
          anchor: 'bottom',
          offset: offsets.get(facility.id) ?? [0, 0],
        })
          .setLngLat([facility.longitude, facility.latitude])
          .addTo(map);

        extractedMarkersRef.current.push(marker);
      });
    };

    renderExtractedMarkers();
    map.on('moveend', renderExtractedMarkers);
    map.on('zoomend', renderExtractedMarkers);

    return () => {
      map.off('moveend', renderExtractedMarkers);
      map.off('zoomend', renderExtractedMarkers);
      extractedMarkersRef.current.forEach((marker) => marker.remove());
      extractedMarkersRef.current = [];
    };
  }, [extractedMapMarkers, onFacilitySelect, selectedFacilityId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || extractedMapMarkers.length === 0) {
      return;
    }

    const validMarkers = extractedMapMarkers.filter(
      (marker) => Number.isFinite(marker.latitude) && Number.isFinite(marker.longitude),
    );

    if (validMarkers.length === 0) {
      return;
    }

    if (validMarkers.length === 1) {
      map.flyTo({
        center: [validMarkers[0].longitude, validMarkers[0].latitude],
        zoom: 12.8,
        essential: true,
        speed: 0.85,
      });
      return;
    }

    const cluster = selectPrimaryExtractedCluster(validMarkers);
    const bounds = new mapboxgl.LngLatBounds();
    cluster.forEach((marker) => {
      bounds.extend([marker.longitude, marker.latitude]);
    });

    map.fitBounds(bounds, {
      padding:
        window.innerWidth < 921
          ? { top: 110, right: 56, bottom: 96, left: 56 }
          : { top: 130, right: 110, bottom: 100, left: 110 },
      duration: 950,
      maxZoom: 12.8,
    });
  }, [extractedMapMarkers]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !selectedFacility || selectedFacility.latitude === null || selectedFacility.longitude === null) {
      return;
    }

    loadingPinMarkerRef.current?.remove();
    loadingPinMarkerRef.current = null;
    loadingLabelMarkerRef.current?.remove();
    loadingLabelMarkerRef.current = null;

    focusMarkerRef.current?.remove();

    const focusElement = createFocusMarkerElement(
      `${selectedFacility.facility_name} selected facility`,
    );

    focusMarkerRef.current = new mapboxgl.Marker({
      element: focusElement,
      anchor: 'center',
    })
      .setLngLat([selectedFacility.longitude, selectedFacility.latitude])
      .addTo(map);

    map.flyTo({
      center: [selectedFacility.longitude, selectedFacility.latitude],
      zoom: 13.4,
      speed: 0.9,
      essential: true,
    });
  }, [selectedFacility]);

  useEffect(() => {
    if (selectedFacilityId) {
      return;
    }

    focusMarkerRef.current?.remove();
    focusMarkerRef.current = null;
    loadingPinMarkerRef.current?.remove();
    loadingPinMarkerRef.current = null;
    loadingLabelMarkerRef.current?.remove();
    loadingLabelMarkerRef.current = null;
  }, [selectedFacilityId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }

    loadingPinMarkerRef.current?.remove();
    loadingPinMarkerRef.current = null;
    loadingLabelMarkerRef.current?.remove();
    loadingLabelMarkerRef.current = null;

    if (
      !isFacilityLoading ||
      !selectedFacilityPreview ||
      selectedFacilityPreview.latitude === null ||
      selectedFacilityPreview.longitude === null
    ) {
      return;
    }

    const lngLat: [number, number] = [
      selectedFacilityPreview.longitude,
      selectedFacilityPreview.latitude,
    ];

    loadingPinMarkerRef.current = new mapboxgl.Marker({
      element: createLoadingPinElement(selectedFacilityPreview.facility_name),
      anchor: 'bottom',
    })
      .setLngLat(lngLat)
      .addTo(map);

    loadingLabelMarkerRef.current = new mapboxgl.Marker({
      element: createLoadingLabelElement(selectedFacilityPreview.facility_name),
      anchor: 'left',
      offset: [28, -46],
    })
      .setLngLat(lngLat)
      .addTo(map);
  }, [isFacilityLoading, selectedFacilityPreview]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }

    const resizeId = window.setTimeout(() => {
      map.resize();
      syncViewport();
    }, 320);

    return () => {
      window.clearTimeout(resizeId);
    };
  }, [chatOpen, sidebarOpen]);

  useEffect(() => {
    const currentMap = mapRef.current;
    if (!currentMap) {
      return;
    }
    const mapInstance = currentMap;

    const query = [filters.city, filters.region, 'Ghana']
      .filter(Boolean)
      .join(', ');

    if (!filters.region) {
      mapInstance.fitBounds(GHANA_BOUNDS, {
        padding: { top: 110, right: 80, bottom: 80, left: 80 },
        duration: 900,
      });

      const source = mapInstance.getSource('selection-focus') as mapboxgl.GeoJSONSource | undefined;
      source?.setData({
        type: 'FeatureCollection',
        features: [],
      });
      return;
    }

    let cancelled = false;

    async function focusRegion() {
      const result = await geocodePlace(query);
      if (cancelled || !mapRef.current) {
        return;
      }

      if (result?.bbox) {
        mapInstance.fitBounds(
          [
            [result.bbox[1], result.bbox[0]],
            [result.bbox[3], result.bbox[2]],
          ],
          {
            padding: { top: 130, right: 100, bottom: 100, left: 100 },
            duration: 950,
          },
        );
      } else if (result?.center) {
        mapInstance.flyTo({
          center: result.center,
          zoom: filters.city ? 11.25 : 8.1,
          essential: true,
          speed: 0.8,
        });
      }

      const highlight = getHighlightFeature(result);
      const source = mapInstance.getSource('selection-focus') as mapboxgl.GeoJSONSource | undefined;
      source?.setData({
        type: 'FeatureCollection',
        features: highlight ? [highlight] : [],
      });
    }

    void focusRegion();

    return () => {
      cancelled = true;
    };
  }, [filters.city, filters.region]);

  return (
    <section className="relative h-screen min-h-[100svh] w-full overflow-hidden">
      {MAPBOX_TOKEN ? (
        <div
          ref={mapNodeRef}
          className="absolute inset-0 h-full w-full min-h-full min-w-full animate-map-in"
        />
      ) : (
        <div className="absolute inset-0 grid gap-2 rounded-panel bg-surface-panel p-6 text-ink-600 shadow-inset-soft">
          <strong className="text-ink-900">Mapbox token missing</strong>
          <p>
            Add `MAPBOX_TOKEN` or `VITE_MAPBOX_TOKEN` in `frontend/.env` to render the
            interactive Ghana map.
          </p>
        </div>
      )}

      {/* <div className="absolute bottom-8 right-7 z-[12] grid max-w-[270px] gap-1 rounded-pill bg-white/88 px-5 py-4.5 shadow-overlay backdrop-blur-[12px] max-[920px]:hidden">
        <div className="text-overline uppercase tracking-[0.14em] text-accent-600">Focused map</div>
        <strong className="text-[1.12rem] text-ink-900">{filters.city || filters.region || 'Ghana'}</strong>
        <span className="text-ui text-ink-600">
          {filters.region
            ? `Pins show ${facilities.length} healthcare profiles in the current view.`
            : 'Select a region to load facilities and place markers.'}
        </span>
      </div> */}

      <div className="pointer-events-none absolute right-9 top-[120px] z-[5] max-[920px]:hidden">
        <div className="flex items-center justify-end gap-4 text-[rgba(25,47,77,0.08)]">
          <div className="text-right text-[clamp(3rem,8vw,7rem)] font-extrabold uppercase leading-[0.92] tracking-[-0.06em]">
            {formatLabel(filters.city || filters.region || 'Ghana')}
          </div>
          <Search className="size-16 shrink-0" strokeWidth={1.5} />
        </div>
      </div>
    </section>
  );
}
