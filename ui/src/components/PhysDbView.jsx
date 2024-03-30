import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/PhysDbView.css";
import {
  highlightTableViewClass,
  highlightEngineViewClass,
  sortTablesToHoist,
} from "../highlight";

function PhysDbView({
  name,
  provisioning,
  tables,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
  nextEngine,
}) {
  const physDbName = name;
  const sortedTables = sortTablesToHoist(highlight, physDbName, false, tables);

  return (
    <div
      class={`physdb-view ${highlightEngineViewClass(highlight, physDbName, false)}`}
    >
      <DbCylinder color="blue">{name}</DbCylinder>
      <div class="physdb-view-prov">{provisioning}</div>
      {nextEngine && (
        <div class="physdb-view-prov transition">
          {nextEngine.provisioning ? "→ " : ""}
          {nextEngine.provisioning}
        </div>
      )}
      <div class="db-table-set">
        {sortedTables.map(({ name, is_writer, mapped_to }) => (
          <TableView
            key={name}
            name={name}
            isWriter={is_writer}
            color="blue"
            highlightClass={highlightTableViewClass(
              highlight,
              physDbName,
              name,
              false,
            )}
            onTableHoverEnter={() =>
              onTableHoverEnter(physDbName, name, false, mapped_to)
            }
            onTableHoverExit={onTableHoverExit}
          />
        ))}
      </div>
    </div>
  );
}

export default PhysDbView;
