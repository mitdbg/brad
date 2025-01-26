import { useEffect, useState, useRef, useCallback } from "react";
import { fetchMetrics } from "../api";
import MetricsManager from "../metrics";
import Panel from "./Panel";
import TroubleshootRoundedIcon from "@mui/icons-material/TroubleshootRounded";
import VdbeMetricsView from "./VdbeMetricsView";
import "./styles/PerfView.css";

const REFRESH_INTERVAL_MS = 30 * 1000;

function extractMetrics({ metrics }, metricName, multiplier) {
  if (multiplier == null) {
    multiplier = 1.0;
  }
  if (!metrics.hasOwnProperty(metricName)) {
    return {
      x: [],
      y: [],
    };
  } else {
    const innerMetrics = metrics[metricName];
    return {
      x: innerMetrics.timestamps.map((val) => val.toLocaleTimeString("en-US")),
      y: innerMetrics.values.map((val) => val * multiplier),
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

function WindowSelector({ windowSizeMinutes, onWindowSizeChange }) {
  function className(windowSizeOption) {
    return `perf-view-winsel-button ${windowSizeOption === windowSizeMinutes ? "selected" : ""}`;
  }
  return (
    <div class="perf-view-winsel">
      Show metrics for last
      <button
        class={className(10)}
        style={{ marginLeft: "20px" }}
        onClick={() => onWindowSizeChange(10)}
      >
        10 mins
      </button>
      <button class={className(30)} onClick={() => onWindowSizeChange(30)}>
        30 mins
      </button>
      <button class={className(60)} onClick={() => onWindowSizeChange(60)}>
        60 mins
      </button>
    </div>
  );
}

function PerfView({ virtualInfra, showingPreview }) {
  const [windowSizeMinutes, setWindowSizeMinutes] = useState(10);
  const [metricsData, setMetricsData] = useState({
    windowSizeMinutes,
    metrics: {},
  });

  const metricsManagerRef = useRef(null);
  function getMetricsManager() {
    if (metricsManagerRef.current == null) {
      metricsManagerRef.current = new MetricsManager();
    }
    return metricsManagerRef.current;
  }

  const refreshData = useCallback(async () => {
    const rawMetrics = await fetchMetrics(60, /*useGenerated=*/ false);
    const fetchedMetrics = parseMetrics(rawMetrics);
    const metricsManager = getMetricsManager();
    const addedNewMetrics = metricsManager.mergeInMetrics(fetchedMetrics);
    if (addedNewMetrics) {
      setMetricsData({
        windowSizeMinutes,
        metrics: metricsManager.getMetricsInWindow(
          windowSizeMinutes,
          /*extendForward=*/ true,
        ),
      });
    }
  }, [metricsManagerRef, windowSizeMinutes, setMetricsData]);

  useEffect(() => {
    // Run first fetch immediately.
    refreshData();
    const intervalId = setInterval(refreshData, REFRESH_INTERVAL_MS);
    return () => {
      if (intervalId === null) {
        return;
      }
      clearInterval(intervalId);
    };
  }, [refreshData]);

  if (metricsData.windowSizeMinutes !== windowSizeMinutes) {
    const metricsManager = getMetricsManager();
    setMetricsData({
      windowSizeMinutes,
      metrics: metricsManager.getMetricsInWindow(
        windowSizeMinutes,
        /*extendForward=*/ true,
      ),
    });
  }

  const queryLatMetrics = extractMetrics(metricsData, "query_latency_s_p90");
  const txnLatMetrics = extractMetrics(metricsData, "txn_latency_s_p90");

  const columnStyle = {
    flexGrow: 2,
  };
  if (showingPreview) {
    columnStyle.opacity = 0.333;
  }

  return (
    <div class="column" style={columnStyle}>
      <div class="perf-view-heading">
        <h2 class="col-h2">
          <TroubleshootRoundedIcon style={{ marginRight: "8px" }} />
          Performance Monitoring
        </h2>
        <WindowSelector
          windowSizeMinutes={windowSizeMinutes}
          onWindowSizeChange={setWindowSizeMinutes}
        />
      </div>
      <div class="column-inner">
        <Panel>
          <div class="perf-view-wrap">
            {virtualInfra?.engines?.map((vdbe, idx) => (
              <VdbeMetricsView
                key={vdbe.internal_id}
                vdbe={vdbe}
                metrics={idx === 0 ? txnLatMetrics : queryLatMetrics}
              />
            ))}
          </div>
        </Panel>
      </div>
    </div>
  );
}

export default PerfView;
