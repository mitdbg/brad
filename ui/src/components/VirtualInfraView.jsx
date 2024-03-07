import Panel from "./Panel";
import VdbeView from "./VdbeView";

function VirtualInfraView() {
  return (
    <Panel heading="Virtual Databases">
      <VdbeView name="VDBE 1" />
    </Panel>
  );
}

export default VirtualInfraView;
