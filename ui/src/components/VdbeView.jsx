import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/VdbeView.css";

function VdbeView({ name, freshness, dialect, performance, tables }) {
  return (
    <div class="vdbe-view">
      <DbCylinder color="green">{name}</DbCylinder>
      <div class="vdbe-view-props">
        <ul>
          <li>🌿: {freshness}</li>
          <li>⏱️: Query Latency {performance}</li>
          <li>🗣: {dialect}</li>
        </ul>
      </div>
      <div class="db-table-set">
        {tables.map(({ name, is_writer }) => (
          <TableView
            key={name}
            name={name}
            isWriter={is_writer}
            color="green"
          />
        ))}
      </div>
    </div>
  );
}

export default VdbeView;
