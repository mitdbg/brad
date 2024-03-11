import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/PhysDbView.css";

function PhysDbView({ name, provisioning, tables }) {
  return (
    <div class="physdb-view">
      <DbCylinder color="blue">{name}</DbCylinder>
      <div class="physdb-view-prov">{provisioning}</div>
      {tables.map(({ name, is_writer }) => (
        <TableView key={name} name={name} isWriter={is_writer} color="blue" />
      ))}
    </div>
  );
}

export default PhysDbView;
