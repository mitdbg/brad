import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/VdbeView.css";

function VdbeView({ name }) {
  return (
    <div class="vdbe-view">
      <DbCylinder color="blue">{name}</DbCylinder>
      <div class="vdbe-view-props">
        <ul>
          <li>🌿: Serializable</li>
          <li>{`⏱️: Query Latency < 30 ms`}</li>
          <li>🗣: PostgreSQL SQL</li>
        </ul>
      </div>
      <TableView name="Table 1" isWriter={true} color="blue" />
      <TableView name="Table 2" />
      <TableView name="Table 3" />
    </div>
  );
}

export default VdbeView;
