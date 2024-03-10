import Panel from "./Panel";
import LatencyPlot from "./LatencyPlot";
import "./styles/PerfView.css";

function extractMetrics(data, metricName, multiplier) {
  if (multiplier == null) {
    multiplier = 1.0;
  }
  if (!data.hasOwnProperty(metricName)) {
    return {
      x: [],
      y: [],
    };
  } else {
    const metrics = data[metricName];
    return {
      x: metrics.timestamps.map((_, idx) => idx),
      y: metrics.values.map((val) => val * multiplier),
    };
  }
}

function PerfView({ metricsData }) {
  const queryLatMetrics = extractMetrics(metricsData, "query_latency_s_p90");
  const txnLatMetrics = extractMetrics(metricsData, "txn_latency_s_p90");
  return (
    <Panel>
      <div class="perf-view-wrap">
        <div>
          <h2>Query Latency</h2>
          <LatencyPlot
            seriesName="Query Latency"
            labels={queryLatMetrics.x}
            values={queryLatMetrics.y}
            xLabel="Elapsed Time (minutes)"
            yLabel="p90 Latency (s)"
          />
        </div>
        <div style={{ marginTop: "30px" }}>
          <h2>Transaction Latency</h2>
          <LatencyPlot
            seriesName="Transaction Latency"
            labels={txnLatMetrics.x}
            values={txnLatMetrics.y}
            xLabel="Elapsed Time (minutes)"
            yLabel="p90 Latency (ms)"
          />
        </div>
      </div>
    </Panel>
  );
}

export default PerfView;
