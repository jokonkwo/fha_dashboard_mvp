"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import { fetchAqiSummary, fetchGeoJSON, fetchSensorCounts } from "@/lib/aqi";
import { API_BASE } from "@/lib/config";

console.log("API_BASE =", API_BASE);

const Plot = dynamic(
  () => import("react-plotly.js").then((m) => m.default),
  { ssr: false }
);


const ISO = (d: Date) => d.toISOString().split(".")[0] + "Z";

export default function Home() {
  const [zip, setZip] = useState("93727");
  const [geo, setGeo] = useState<any>(null);
  const [summary, setSummary] = useState<any>(null);
  const [counts, setCounts] = useState<any>(null);

  const now = useMemo(() => new Date(), []);
  const start = useMemo(() => {
    const d = new Date(now);
    d.setDate(d.getDate() - 3);
    d.setHours(0,0,0,0);
    return ISO(d);
  }, [now]);
  const end = useMemo(() => ISO(now), [now]);

  useEffect(() => {
    fetchGeoJSON().then(setGeo).catch(console.error);
  }, []);
  useEffect(() => {
    fetchAqiSummary(start, end, zip).then(setSummary).catch(console.error);
    fetchSensorCounts(zip).then(setCounts).catch(console.error);
  }, [zip, start, end]);

  const ts = summary?.timeseries ?? [];
  const stats = summary?.stats;
  const plotData = [
  {
    x: ts.map((r: any) => r.timestamp),
    y: ts.map((r: any) => r.aqi),
    type: "scatter" as const,
    mode: "lines" as const,
  },
];

  return (
    <main className="max-w-5xl mx-auto p-6 space-y-6">
      <h1 className="text-2xl font-semibold">Air Quality Dashboard (dev)</h1>

      <div className="flex items-center gap-3">
        <label className="text-sm text-gray-600">ZIP:</label>
        <select
          className="border rounded px-2 py-1"
          value={zip}
          onChange={(e) => setZip(e.target.value)}
        >
          <option>93727</option>
          <option>93720</option>
          <option>93706</option>
        </select>
        {counts && <span className="text-sm text-gray-600">Sensors: {counts.sensors}</span>}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <StatCard label="Mean AQI" value={fmt(stats?.mean)} />
        <StatCard label="P95 AQI" value={fmt(stats?.p95)} />
        <StatCard label="Max AQI" value={fmt(stats?.max)} />
      </div>

      <div className="bg-white rounded-xl p-4 shadow">
        <h2 className="font-medium mb-2">AQI (last 3 days)</h2>
        <div className="overflow-x-auto">
          <Plot
            data={plotData as any}  // keep it simple for now
            layout={{ autosize: true, margin: { l: 40, r: 10, t: 10, b: 40 } }}
            useResizeHandler
            style={{ width: "100%", height: "320px" }}
            config={{ displayModeBar: false }}
          />
        </div>
      </div>

      <div className="bg-white rounded-xl p-4 shadow">
        <h2 className="font-medium mb-2">GeoJSON sample</h2>
        <pre className="text-xs overflow-auto max-h-64 bg-gray-50 p-3 rounded">
          {geo ? JSON.stringify(geo, null, 2) : "Loading..."}
        </pre>
      </div>
    </main>
  );
}

function StatCard({ label, value }: { label: string; value?: string }) {
  return (
    <div className="bg-white rounded-xl p-4 shadow flex flex-col">
      <span className="text-xs text-gray-500">{label}</span>
      <span className="text-2xl font-semibold">{value ?? "—"}</span>
    </div>
  );
}
function fmt(n?: number) {
  if (n == null || Number.isNaN(n)) return undefined;
  return Math.round(n).toString();
}
