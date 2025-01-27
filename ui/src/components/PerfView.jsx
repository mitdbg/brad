import { useCallback, useState, useEffect, useRef } from "react";
import Panel from "./Panel";
import TroubleshootRoundedIcon from "@mui/icons-material/TroubleshootRounded";
import VdbeMetricsView from "./VdbeMetricsView";
import { parseMetrics, extractMetrics } from "../metrics_utils";
import MetricsManager from "../metrics";
import { fetchMetrics } from "../api";
import "./styles/PerfView.css";

const REFRESH_INTERVAL_MS = 30 * 1000;

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

function vdbeWithMetrics(virtualInfra, displayMetricsData, showSpecific) {
  return virtualInfra?.engines?.map((vdbe, idx) => {
    let metrics;
    if (showSpecific) {
      metrics = extractMetrics(displayMetricsData, `vdbe:${vdbe.internal_id}`);
    } else {
      metrics = extractMetrics(
        displayMetricsData,
        idx === 0 ? "txn_latency_s_p90" : "query_latency_s_p90",
      );
    }
    return { vdbe, metrics };
  });
}

function PerfView({ virtualInfra, showingPreview, showVdbeSpecificMetrics }) {
  const metricsManagerRef = useRef(null);
  const getMetricsManager = useCallback(() => {
    if (metricsManagerRef.current == null) {
      metricsManagerRef.current = new MetricsManager();
    }
    return metricsManagerRef.current;
  }, [metricsManagerRef]);

  const [windowSizeMinutes, setWindowSizeMinutes] = useState(10);
  const [displayMetricsData, setDisplayMetricsData] = useState({
    windowSizeMinutes: 10,
    metrics: {},
  });

  if (displayMetricsData.windowSizeMinutes !== windowSizeMinutes) {
    changeDisplayMetricsWindow(windowSizeMinutes);
  }

  const refreshMetrics = useCallback(async () => {
    const rawMetrics = await fetchMetrics(60, /*useGenerated=*/ false);
    const fetchedMetrics = parseMetrics(rawMetrics);
    const metricsManager = getMetricsManager();
    const addedNewMetrics = metricsManager.mergeInMetrics(fetchedMetrics);
    if (addedNewMetrics) {
      const { windowSizeMinutes } = displayMetricsData;
      setDisplayMetricsData({
        windowSizeMinutes,
        metrics: metricsManager.getMetricsInWindow(
          windowSizeMinutes,
          /*extendForward=*/ true,
        ),
      });
    }
  }, [getMetricsManager, displayMetricsData, setDisplayMetricsData]);

  useEffect(() => {
    refreshMetrics();
    const intervalId = setInterval(refreshMetrics, REFRESH_INTERVAL_MS);
    return () => {
      if (intervalId === null) {
        return;
      }
      clearInterval(intervalId);
    };
  }, [refreshMetrics]);

  const changeDisplayMetricsWindow = useCallback(
    (windowSizeMinutes) => {
      const metricsManager = getMetricsManager();
      setDisplayMetricsData({
        windowSizeMinutes,
        metrics: metricsManager.getMetricsInWindow(
          windowSizeMinutes,
          /*extendForward=*/ true,
        ),
      });
    },
    [getMetricsManager, setDisplayMetricsData],
  );

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
            {vdbeWithMetrics(
              virtualInfra,
              displayMetricsData,
              showVdbeSpecificMetrics,
            )?.map(({ vdbe, metrics }) => (
              <VdbeMetricsView
                key={vdbe.internal_id}
                vdbe={vdbe}
                metrics={metrics}
              />
            ))}
          </div>
        </Panel>
      </div>
    </div>
  );
}

export default PerfView;
