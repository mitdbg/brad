import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/VdbeView.css";

function VdbeView({
  name,
  freshness,
  dialect,
  peak_latency_s,
  tables,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
  index,
}) {
  const virtualEngineIndex = index.toString();
  function shouldHighlight(tableName) {
    return highlight.virtualEngines[virtualEngineIndex] === tableName;
  }
  function inHighlightMode() {
    return (
      Object.keys(highlight.virtualEngines).length > 0 ||
      Object.keys(highlight.physicalEngines).length > 0
    );
  }

  return (
    <div class={`vdbe-view ${inHighlightMode() ? "highlight-mode" : ""}`}>
      <DbCylinder color="green">{name}</DbCylinder>
      <div class="vdbe-view-props">
        <ul>
          <li>🌿: {freshness}</li>
          {peak_latency_s && <li>⏱️: Query Latency ≤ {peak_latency_s} s</li>}
          <li>🗣: {dialect}</li>
        </ul>
      </div>
      <div class="db-table-set">
        {tables.map(({ name, is_writer, mapped_to }) => (
          <TableView
            key={name}
            name={name}
            isWriter={is_writer}
            color="green"
            isHighlighted={shouldHighlight(name)}
            onTableHoverEnter={() =>
              onTableHoverEnter(virtualEngineIndex, name, true, mapped_to)
            }
            onTableHoverExit={onTableHoverExit}
          />
        ))}
      </div>
    </div>
  );
}

export default VdbeView;
