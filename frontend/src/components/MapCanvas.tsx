import { useEffect, useEffectEvent, useRef } from 'react';
import mapboxgl from 'mapbox-gl';

import { geocodePlace, getHighlightFeature } from '../lib/api';
import { GHANA_BOUNDS, GHANA_VIEW, formatLabel } from '../lib/format';
import { MAPBOX_TOKEN } from '../lib/env';
import type { BoundingBox, FacilityProfile, FacilitySummary, SearchFilters } from '../lib/types';

interface MapCanvasProps {
  filters: SearchFilters;
  facilities: FacilitySummary[];
  selectedFacility: FacilityProfile | null | undefined;
  selectedFacilityId: string | null;
  hoveredFacilityId: string | null;
  onViewportChange: (bbox: BoundingBox | null) => void;
  onFacilitySelect: (facilityId: string) => void;
}

function createMarkerElement(active: boolean, label: string) {
  const element = document.createElement('button');
  element.type = 'button';
  element.className = `facility-marker ${active ? 'is-active' : ''}`;
  element.setAttribute('aria-label', label);
  element.innerHTML = '<span class="facility-marker__halo"></span><span class="facility-marker__dot"></span>';
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

export function MapCanvas({
  filters,
  facilities,
  selectedFacility,
  selectedFacilityId,
  hoveredFacilityId,
  onViewportChange,
  onFacilitySelect,
}: MapCanvasProps) {
  const mapNodeRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<mapboxgl.Map | null>(null);
  const markersRef = useRef<mapboxgl.Marker[]>([]);

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
      style: 'mapbox://styles/mapbox/light-v11',
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

    return () => {
      markersRef.current.forEach((marker) => marker.remove());
      markersRef.current = [];
      map.remove();
      mapRef.current = null;
    };
  }, []);

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
    if (!map || !selectedFacility || selectedFacility.latitude === null || selectedFacility.longitude === null) {
      return;
    }

    map.flyTo({
      center: [selectedFacility.longitude, selectedFacility.latitude],
      zoom: 12.8,
      speed: 0.9,
      essential: true,
    });
  }, [selectedFacility]);

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
    <section className="map-shell">
      {MAPBOX_TOKEN ? (
        <div ref={mapNodeRef} className="map-surface" />
      ) : (
        <div className="map-fallback">
          <strong>Mapbox token missing</strong>
          <p>
            Add `MAPBOX_TOKEN` or `VITE_MAPBOX_TOKEN` in `frontend/.env` to render the
            interactive Ghana map.
          </p>
        </div>
      )}

      <div className="map-overlay">
        <div className="map-overlay__label">Focused map</div>
        <strong>{filters.city || filters.region || 'Ghana'}</strong>
        <span>
          {filters.region
            ? `Pins show ${facilities.length} healthcare profiles in the current view.`
            : 'Select a region to load facilities and place markers.'}
        </span>
      </div>

      <div className="map-watermark">{formatLabel(filters.city || filters.region || 'Ghana')}</div>
    </section>
  );
}
