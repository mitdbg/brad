import { useState } from "react";
import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import ExpandableTableSet from "./ExpandableTableSet";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import EditRoundedIcon from "@mui/icons-material/EditRounded";
import DeleteRoundedIcon from "@mui/icons-material/DeleteRounded";
import LinkRoundedIcon from "@mui/icons-material/LinkRounded";
import Snackbar from "@mui/material/Snackbar";
import "./styles/VdbeView.css";

function formatMilliseconds(milliseconds) {
  if (milliseconds == null) {
    return null;
  }

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
  if (maxStalenessMs == null) {
    return null;
  }

  if (maxStalenessMs === 0) {
    return "No staleness";
  }
  return `Staleness ≤ ${formatMilliseconds(maxStalenessMs)}`;
}

function formatDialect(queryInterface) {
  if (queryInterface == null) {
    return null;
  }

  if (queryInterface === "postgresql") {
    return "PostgreSQL SQL";
  } else if (queryInterface === "athena") {
    return "Athena SQL";
  } else if (queryInterface === "common") {
    return "SQL-99";
  } else {
    console.error("Unknown", queryInterface);
    return null;
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

function VdbeEndpoint({ endpoint, setShowSnackbar }) {
  const handleCopy = () => {
    if (navigator.clipboard != null) {
      navigator.clipboard.writeText(endpoint);
    }
    setShowSnackbar(true);
  };
  return (
    <div class="vdbe-endpoint" onClick={handleCopy}>
      <LinkRoundedIcon style={{ marginRight: "8px" }} />
      <Tooltip title="Click to copy endpoint" placement="right">
        <span>{endpoint}</span>
      </Tooltip>
    </div>
  );
}

function VdbeView({
  vdbe,
  onTableClick,
  editable,
  onEditClick,
  onDeleteClick,
  hideEndpoint,
}) {
  if (onEditClick == null) {
    onEditClick = () => {};
  }

  const vengName = vdbe.name;
  const tables = vdbe.tables;
  const freshness = formatFreshness(vdbe.max_staleness_ms);
  const peakLatency = formatMilliseconds(vdbe.p90_latency_slo_ms);
  const dialect = formatDialect(vdbe.interface);
  const sortedTables = tables;
  // const sortedTables = sortTablesToHoist(highlight, vengName, true, tables);
  const [showSnackbar, setShowSnackbar] = useState(false);

  const handleClose = (event, reason) => {
    if (reason === "clickaway") {
      return;
    }
    setShowSnackbar(false);
  };

  return (
    <>
      <div className="vdbe-view">
        <div className="vdbe-db-wrap">
          <DbCylinder color="green">{vengName}</DbCylinder>
          {editable && (
            <EditControls
              onEditClick={() => onEditClick(vdbe)}
              onDeleteClick={() => onDeleteClick(vdbe)}
            />
          )}
        </div>
        {vdbe.endpoint && !hideEndpoint && (
          <VdbeEndpoint
            endpoint={vdbe.endpoint}
            setShowSnackbar={setShowSnackbar}
          />
        )}
        <div class="vdbe-view-props">
          <ul>
            <li>🌿: {freshness != null ? freshness : "-----"}</li>
            <li>
              ⏱️:{" "}
              {peakLatency != null
                ? `p90 Query Latency ≤ ${peakLatency}`
                : "-----"}
            </li>
            <li>🗣: {dialect != null ? dialect : "-----"}</li>
          </ul>
        </div>
        <ExpandableTableSet>
          {sortedTables.map(({ name, writable }) => (
            <TableView
              key={name}
              name={name}
              isWriter={writable}
              color="green"
              onTableClick={onTableClick}
            />
          ))}
        </ExpandableTableSet>
      </div>
      <Snackbar
        open={showSnackbar}
        autoHideDuration={2000}
        message="Endpoint copied to clipboard"
        onClose={handleClose}
      />
    </>
  );
}

export default VdbeView;
