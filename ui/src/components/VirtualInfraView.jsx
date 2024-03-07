import Panel from "./Panel";
import DbCylinder from "./DbCylinder";

function VirtualInfraView() {
  return (
    <Panel heading="Virtual Databases">
      <DbCylinder color="blue">VDBE 1</DbCylinder>
    </Panel>
  );
}

export default VirtualInfraView;
