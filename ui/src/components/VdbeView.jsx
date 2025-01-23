import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import ExpandableTableSet from "./ExpandableTableSet";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import EditRoundedIcon from "@mui/icons-material/EditRounded";
import DeleteRoundedIcon from "@mui/icons-material/DeleteRounded";
import "./styles/VdbeView.css";
import {
  highlightTableViewClass,
  highlightEngineViewClass,
  sortTablesToHoist,
} from "../highlight";

function formatMilliseconds(milliseconds) {
  const precision = 2;
  if (milliseconds >= 1000 * 60 * 60) {
    // Use hours.
    const latencyHours = milliseconds / (1000 * 60 * 60);
    return `${latencyHours.toFixed(precision)} hr`;
  } else if (milliseconds >= 1000) {
    // Use milliseconds.
    const latencySeconds = milliseconds / 1000;
    return `${latencySeconds.toFixed(precision)} s`;
  }
  return `${milliseconds} ms`;
}

function formatFreshness(maxStalenessMs) {
  if (maxStalenessMs === 0) {
    return "No staleness";
  }
  return `Staleness ≤ ${formatMilliseconds(maxStalenessMs)}`;
}

function formatDialect(queryInterface) {
  if (queryInterface === "postgresql") {
    return "PostgreSQL SQL";
  } else if (queryInterface === "athena") {
    return "Athena SQL";
  } else if (queryInterface === "common") {
    return "SQL-99";
  }
}

function EditControls({ onEditClick, onDeleteClick }) {
  return (
    <div className="vdbe-edit-controls">
      <Tooltip title="Edit" placement="right">
        <IconButton onClick={onEditClick} className="vdbe-edit-button">
          <EditRoundedIcon />
        </IconButton>
      </Tooltip>
      <Tooltip title="Delete" placement="right">
        <IconButton onClick={onDeleteClick} className="vdbe-edit-button">
          <DeleteRoundedIcon />
        </IconButton>
      </Tooltip>
    </div>
  );
}

function VdbeView({ vdbe, highlight, onTableHoverEnter, onTableHoverExit }) {
  const vengName = vdbe.name;
  const tables = vdbe.tables;
  const freshness = formatFreshness(vdbe.max_staleness_ms);
  const peakLatency = formatMilliseconds(vdbe.p90_latency_slo_ms);
  const dialect = formatDialect(vdbe.interface);
  const sortedTables = sortTablesToHoist(highlight, vengName, true, tables);

  return (
    <div
      class={`vdbe-view ${highlightEngineViewClass(highlight, vengName, true)}`}
    >
      <div className="vdbe-db-wrap">
        <DbCylinder color="green">{vengName}</DbCylinder>
        <EditControls onEditClick={() => {}} onDeleteClick={() => {}} />
      </div>
      <div class="vdbe-view-props">
        <ul>
          <li>🌿: {freshness}</li>
          <li>⏱️: p90 Query Latency ≤ {peakLatency}</li>
          <li>🗣: {dialect}</li>
        </ul>
      </div>
      <ExpandableTableSet>
        {sortedTables.map(({ name, writable, mapped_to }) => (
          <TableView
            key={name}
            name={name}
            isWriter={writable}
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
      </ExpandableTableSet>
    </div>
  );
}

export default VdbeView;
