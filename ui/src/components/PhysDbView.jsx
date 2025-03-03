import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import ExpandableTableSet from "./ExpandableTableSet";
import "./styles/PhysDbView.css";

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

function PhysDbView({ name, provisioning, tables, nextEngine }) {
  const sortedTables = tables;
  const addedTablesList = addedTables(tables, nextEngine);

  const sortedTableComponents = sortedTables.map(({ name, writable }) => (
    <TableView key={name} name={name} isWriter={writable} color="blue" />
  ));
  const addedTableComponents = addedTablesList.map(({ name, writable }) => (
    <TableView
      key={name}
      name={name}
      isWriter={writable}
      color="blue"
      highlightClass="dim"
    />
  ));

  return (
    <div className="physdb-view">
      <DbCylinder color="blue">{name}</DbCylinder>
      <div class="physdb-view-prov">{provisioning}</div>
      {nextEngine && (
        <div class="physdb-view-prov transition">
          {nextEngine.provisioning ? "→ " : ""}
          {nextEngine.provisioning}
        </div>
      )}
      <ExpandableTableSet>
        {[...sortedTableComponents, ...addedTableComponents]}
      </ExpandableTableSet>
    </div>
  );
}

export default PhysDbView;
