import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/PhysDbView.css";

function PhysDbView({ name, provisioning }) {
  return (
    <div class="physdb-view">
      <DbCylinder color="blue">{name}</DbCylinder>
      <div class="physdb-view-prov">
        {provisioning}
      </div>
      <TableView name="Table 1" isWriter={true} color="blue" />
      <TableView name="Table 2" />
      <TableView name="Table 3" />
    </div>
  );
}

export default PhysDbView;
