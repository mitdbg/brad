import Panel from "./Panel";
import VdbeView from "./VdbeView";
import "./styles/VirtualInfraView.css";

function VirtualInfraView() {
  return (
    <Panel heading="Virtual Database Engines">
      <div class="vdbe-view-wrap">
        <VdbeView name="VDBE 1" />
        <VdbeView name="VDBE 2" />
      </div>
    </Panel>
  );
}

export default VirtualInfraView;
