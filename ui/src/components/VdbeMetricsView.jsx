import LatencyPlot from "./LatencyPlot";

function VdbeMetricsView({ vdbe, metrics }) {
  const vdbePeak = vdbe.p90_latency_slo_ms / 1000;
  return (
    <div class="perf-view-plot-wrap">
      <h2>{vdbe.name} VDBE Query Latency</h2>
      <LatencyPlot
        seriesName={`${vdbe.name} VDBE Query Latency`}
        labels={metrics.x}
        values={metrics.y}
        xLabel="Time"
        yLabel="p90 Latency (s)"
        shadeSeconds={vdbePeak}
      />
    </div>
  );
}

export default VdbeMetricsView;
