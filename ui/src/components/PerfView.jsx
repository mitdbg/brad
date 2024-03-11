import { useEffect, useState, useRef } from "react";
import { fetchMetrics } from "../api";
import MetricsManager from "../metrics";
import Panel from "./Panel";
import LatencyPlot from "./LatencyPlot";
import "./styles/PerfView.css";

const REFRESH_INTERVAL_MS = 30 * 1000;

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
      x: metrics.timestamps.map((val) => val.toLocaleTimeString("en-US")),
      y: metrics.values.map((val) => val * multiplier),
    };
  }
}

function parseMetrics({ named_metrics }) {
  const result = {};
  Object.entries(named_metrics).forEach(([metricName, metricValues]) => {
    const parsedTs = metricValues.timestamps.map(
      (timestamp) => new Date(timestamp),
    );
    result[metricName] = {
      timestamps: parsedTs,
      values: metricValues.values,
    };
  });
  return result;
}

function PerfView() {
  const [windowSize, setWindowSize] = useState(10);
  const [metricsData, setMetricsData] = useState({});
  const metricsManagerRef = useRef(null);
  function getMetricsManager() {
    if (metricsManagerRef.current == null) {
      metricsManagerRef.current = new MetricsManager();
    }
    return metricsManagerRef.current;
  }

  useEffect(() => {
    let timeoutId = null;
    const refreshData = async () => {
      const rawMetrics = await fetchMetrics(60);
      const fetchedMetrics = parseMetrics(rawMetrics);
      const metricsManager = getMetricsManager();
      const addedNewMetrics = metricsManager.mergeInMetrics(fetchedMetrics);
      if (addedNewMetrics) {
        setMetricsData(
          metricsManager.getMetricsInWindow(
            windowSize,
            /*extendForward=*/ true,
          ),
        );
      }
      timeoutId = setTimeout(refreshData, REFRESH_INTERVAL_MS);
    };

    // Run first fetch immediately.
    timeoutId = setTimeout(refreshData, 0);
    return () => {
      if (timeoutId === null) {
        return;
      }
      clearTimeout(timeoutId);
    };
  }, [metricsData, windowSize]);

  const queryLatMetrics = extractMetrics(metricsData, "query_latency_s_p90");
  const txnLatMetrics = extractMetrics(
    metricsData,
    "txn_latency_s_p90",
    /*multiplier=*/ 1000,
  );

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
