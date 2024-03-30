import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import WorkloadAdjuster from "./WorkloadAdjuster";
import "./styles/VdbeView.css";
import {
  highlightTableViewClass,
  highlightEngineViewClass,
  sortTablesToHoist,
} from "../highlight";
import { useState, useCallback } from "react";

function formatLatencySeconds(latencySeconds) {
  const precision = 1;
  if (latencySeconds < 1.0) {
    // Use milliseconds.
    const latencyMs = latencySeconds * 1000;
    return `${latencyMs.toFixed(precision)} ms`;
  }
  return `${latencySeconds.toFixed(precision)} s`;
}

function VdbeView({
  name,
  freshness,
  dialect,
  peak_latency_s,
  tables,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
  workloadState,
  updateWorkloadNumClients,
}) {
  const vengName = name;
  const sortedTables = sortTablesToHoist(highlight, vengName, true, tables);

  const [showWorkloadAdjuster, setShowWorkloadAdjuster] = useState(false);
  const toggleWorkloadAdjuster = useCallback(() => {
    setShowWorkloadAdjuster(!showWorkloadAdjuster);
  }, [showWorkloadAdjuster]);

  return (
    <div
      class={`vdbe-view ${highlightEngineViewClass(highlight, vengName, true)}`}
    >
      {workloadState && showWorkloadAdjuster && (
        <WorkloadAdjuster
          min={0}
          max={workloadState.max_clients}
          value={workloadState.curr_clients}
          onChange={updateWorkloadNumClients}
          debounceMs={800}
        />
      )}
      <DbCylinder color="green" onClick={toggleWorkloadAdjuster}>
        {vengName}
      </DbCylinder>
      <div class="vdbe-view-props">
        <ul>
          <li>🌿: {freshness}</li>
          {peak_latency_s && (
            <li>⏱️: Query Latency ≤ {formatLatencySeconds(peak_latency_s)}</li>
          )}
          <li>🗣: {dialect}</li>
        </ul>
      </div>
      <div class="db-table-set">
        {sortedTables.map(({ name, is_writer, mapped_to }) => (
          <TableView
            key={name}
            name={name}
            isWriter={is_writer}
            color="green"
            highlightClass={highlightTableViewClass(
              highlight,
              vengName,
              name,
              true,
            )}
            onTableHoverEnter={() =>
              onTableHoverEnter(vengName, name, true, mapped_to)
            }
            onTableHoverExit={onTableHoverExit}
          />
        ))}
      </div>
    </div>
  );
}

export default VdbeView;
