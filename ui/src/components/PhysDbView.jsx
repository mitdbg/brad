import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/PhysDbView.css";

function PhysDbView({
  name,
  provisioning,
  tables,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
}) {
  const physDbName = name;
  function shouldHighlight(tableName) {
    return highlight.physicalEngines[name] === tableName;
  }
  function inHighlightMode() {
    return (
      Object.keys(highlight.virtualEngines).length > 0 ||
      Object.keys(highlight.physicalEngines).length > 0
    );
  }

  return (
    <div class={`physdb-view ${inHighlightMode() ? "highlight-mode" : ""}`}>
      <DbCylinder color="blue">{name}</DbCylinder>
      <div class="physdb-view-prov">{provisioning}</div>
      <div class="db-table-set">
        {tables.map(({ name, is_writer, mapped_to }) => (
          <TableView
            key={name}
            name={name}
            isWriter={is_writer}
            color="blue"
            isHighlighted={shouldHighlight(name)}
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
