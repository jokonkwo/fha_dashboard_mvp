import { API_BASE } from "./config";

export async function fetchGeoJSON() {
  const res = await fetch(`${API_BASE}/geojson`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load geojson");
  return res.json();
}

export async function fetchAqiSummary(
  startISO: string,
  endISO: string,
  zip?: string
) {
  const url = new URL(`${API_BASE}/aqi-summary`);
  url.searchParams.set("start", startISO);
  url.searchParams.set("end", endISO);
  if (zip) url.searchParams.set("zip", zip);
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load aqi");
  return res.json();
}

export async function fetchSensorCounts(zip: string) {
  const url = new URL(`${API_BASE}/sensor-counts`);
  url.searchParams.set("zip", zip);
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load counts");
  return res.json();
}
