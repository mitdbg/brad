import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/PhysDbView.css";
import {
  highlightTableViewClass,
  highlightEngineViewClass,
  sortTablesToHoist,
} from "../highlight";

function addedTables(tables, nextEngine) {
  if (nextEngine == null) return [];
  const added = [];
  const currTableSet = new Set();
  for (const currTable of tables) {
    currTableSet.add(currTable.name);
  }
  for (const table of nextEngine.tables) {
    if (currTableSet.has(table.name)) continue;
    added.push(table);
  }
  return added;
}

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
  const addedTablesList = addedTables(tables, nextEngine);

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
        {sortedTables.map(({ name, writable, mapped_to }) => (
          <TableView
            key={name}
            name={name}
            isWriter={writable}
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
        {addedTablesList.map(({ name, writable }) => (
          <TableView
            key={name}
            name={name}
            isWriter={writable}
            color="blue"
            highlightClass="dim"
            onTableHoverEnter={() => {}}
            onTableHoverExit={() => {}}
          />
        ))}
      </div>
    </div>
  );
}

export default PhysDbView;
