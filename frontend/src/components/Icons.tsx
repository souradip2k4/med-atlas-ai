import type { ReactNode } from 'react';

interface IconProps {
  className?: string;
}

function wrapPath(path: ReactNode, className?: string) {
  return (
    <svg className={className} viewBox="0 0 24 24" aria-hidden="true">
      {path}
    </svg>
  );
}

export function RegionIcon({ className }: IconProps) {
  return wrapPath(
    <>
      <path
        fill="currentColor"
        d="M6.54 3.54a1.5 1.5 0 0 1 2.12 0l2.18 2.18a1.5 1.5 0 0 1 .33 1.63l-.62 1.57 1.93 1.92 2.2-.63a1.5 1.5 0 0 1 1.47.4l3.35 3.35a1.5 1.5 0 0 1-.37 2.39l-6.87 3.85a1.5 1.5 0 0 1-1.83-.25L4.1 13.6a1.5 1.5 0 0 1-.25-1.83l3.85-6.87a1.5 1.5 0 0 1 .84-.7Z"
      />
      <path
        fill="currentColor"
        opacity="0.35"
        d="M4.35 17.15a1.5 1.5 0 0 1 2.12 0l1.38 1.38a1.5 1.5 0 0 1 0 2.12l-1.38 1.38a1.5 1.5 0 0 1-2.12 0L2.97 20.65a1.5 1.5 0 0 1 0-2.12l1.38-1.38Z"
      />
    </>,
    className,
  );
}

export function CityIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="M12 2a7 7 0 0 1 7 7c0 5.26-5.04 11.4-6.22 12.77a1 1 0 0 1-1.56 0C10.04 20.4 5 14.26 5 9a7 7 0 0 1 7-7Zm0 3.2A3.8 3.8 0 1 0 12 12.8a3.8 3.8 0 0 0 0-7.6Z"
    />,
    className,
  );
}

export function SpecialtyIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="M12 3a3 3 0 0 1 3 3v2.38a4 4 0 0 1 1.58 7.45l2.09 3.63a1 1 0 1 1-1.73 1l-2.1-3.64a3.98 3.98 0 0 1-5.68 0l-2.1 3.64a1 1 0 1 1-1.73-1l2.09-3.63A4 4 0 0 1 9 8.38V6a3 3 0 0 1 3-3Zm0 2a1 1 0 0 0-1 1v2h2V6a1 1 0 0 0-1-1Zm0 5a2 2 0 1 0 0 4 2 2 0 0 0 0-4Z"
    />,
    className,
  );
}

export function SearchIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="M10.5 4a6.5 6.5 0 1 1 0 13 6.5 6.5 0 0 1 0-13Zm0 2a4.5 4.5 0 1 0 2.82 8.01l3.33 3.33a1 1 0 0 0 1.41-1.41l-3.33-3.33A4.5 4.5 0 0 0 10.5 6Z"
    />,
    className,
  );
}

export function CloseIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="m6.7 5.3 5.3 5.3 5.3-5.3a1 1 0 1 1 1.4 1.4L13.4 12l5.3 5.3a1 1 0 1 1-1.4 1.4L12 13.4l-5.3 5.3a1 1 0 1 1-1.4-1.4l5.3-5.3-5.3-5.3a1 1 0 0 1 1.4-1.4Z"
    />,
    className,
  );
}

export function SlidersIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="M6 4a1 1 0 0 1 1 1v2h10a1 1 0 1 1 0 2H7v1a1 1 0 0 1-2 0V5a1 1 0 0 1 1-1Zm12 10a1 1 0 0 1 1 1v4a1 1 0 1 1-2 0v-1H7a1 1 0 1 1 0-2h10v-1a1 1 0 0 1 1-1ZM12 9a1 1 0 0 1 1 1v4a1 1 0 1 1-2 0v-4a1 1 0 0 1 1-1Z"
    />,
    className,
  );
}

export function LocationPinIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="M12 2a6 6 0 0 1 6 6c0 4.55-4.11 9.62-5.08 10.76a1.2 1.2 0 0 1-1.84 0C10.11 17.62 6 12.55 6 8a6 6 0 0 1 6-6Zm0 3.1A2.9 2.9 0 1 0 12 10.9a2.9 2.9 0 0 0 0-5.8Z"
    />,
    className,
  );
}

export function PhoneIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="M7.14 3.5a1 1 0 0 1 1.05.24l2 2a1 1 0 0 1 .2 1.12L9.3 9.36a14.73 14.73 0 0 0 5.34 5.34l2.5-1.1a1 1 0 0 1 1.12.2l2 2a1 1 0 0 1 .24 1.05c-.24.9-.87 1.73-1.78 2.1-.97.4-2.6.49-4.84-.58-2.12-1.02-4.55-2.92-6.93-5.3-2.38-2.38-4.28-4.81-5.3-6.93-1.07-2.24-.98-3.87-.58-4.84.37-.91 1.2-1.54 2.1-1.78Z"
    />,
    className,
  );
}

export function GlobeIcon({ className }: IconProps) {
  return wrapPath(
    <path
      fill="currentColor"
      d="M12 3a9 9 0 1 1 0 18 9 9 0 0 1 0-18Zm6.86 8h-3.07a15.02 15.02 0 0 0-1.1-4.04A7.02 7.02 0 0 1 18.86 11ZM12 5c-.79 0-1.94 1.77-2.35 4h4.7C13.94 6.77 12.79 5 12 5Zm-2.57 6A13.1 13.1 0 0 0 9.5 13c0 .67.05 1.34.16 2h4.68c.11-.66.16-1.33.16-2s-.05-1.34-.16-2H9.43Zm.22 6a15.02 15.02 0 0 0 1.1 4.04A7.02 7.02 0 0 1 5.14 13h3.07c.19 1.39.56 2.76 1.1 4Zm4.7 0h-4.7c.41 2.23 1.56 4 2.35 4 .79 0 1.94-1.77 2.35-4Zm.34-2h3.07a7.02 7.02 0 0 1-4.17 4.04c.54-1.28.91-2.65 1.1-4.04ZM8.21 11H5.14a7.02 7.02 0 0 1 4.17-4.04A15.02 15.02 0 0 0 8.2 11Z"
    />,
    className,
  );
}
