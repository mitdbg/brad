import DbCylinder from "./DbCylinder";
import TableView from "./TableView";
import "./styles/PhysDbView.css";
import { highlightTableViewClass } from "../highlight";

function PhysDbView({
  name,
  provisioning,
  tables,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
}) {
  const physDbName = name;

  return (
    <div class="physdb-view">
      <DbCylinder color="blue">{name}</DbCylinder>
      <div class="physdb-view-prov">{provisioning}</div>
      <div class="db-table-set">
        {tables.map(({ name, is_writer, mapped_to }) => (
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
