import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/VdbeView.css";
import {
  highlightTableViewClass,
  highlightEngineViewClass,
  sortTablesToHoist,
} from "../highlight";

function VdbeView({
  name,
  freshness,
  dialect,
  peak_latency_s,
  tables,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
}) {
  const vengName = name;
  const sortedTables = sortTablesToHoist(highlight, vengName, true, tables);

  return (
    <div
      class={`vdbe-view ${highlightEngineViewClass(highlight, vengName, true)}`}
    >
      <DbCylinder color="green">{vengName}</DbCylinder>
      <div class="vdbe-view-props">
        <ul>
          <li>🌿: {freshness}</li>
          {peak_latency_s && <li>⏱️: Query Latency ≤ {peak_latency_s} s</li>}
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
