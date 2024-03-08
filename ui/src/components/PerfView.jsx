import Panel from "./Panel";
import LatencyPlot from "./LatencyPlot";
import "./styles/PerfView.css";

function extractMetrics(data, metricName) {
  if (!data.hasOwnProperty(metricName)) {
    return {
      x: [],
      y: [],
    };
  } else {
    const metrics = data[metricName];
    return {
      x: metrics.timestamps.map((_, idx) => idx),
      y: metrics.values,
    };
  }
}

function PerfView({ metricsData }) {
  const latencyMetrics = extractMetrics(metricsData, "query_latency_s_p90");
  return (
    <Panel>
      <div class="perf-view-wrap">
        <h2>VDBE 1: Query Latency</h2>
        <LatencyPlot
          seriesName="VDBE 1"
          labels={latencyMetrics.x}
          values={latencyMetrics.y}
        />
      </div>
    </Panel>
  );
}

export default PerfView;
