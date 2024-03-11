import Panel from "./Panel";
import VdbeView from "./VdbeView";
import "./styles/VirtualInfraView.css";

function VirtualInfraView({ virtualInfra }) {
  return (
    <Panel heading="Virtual Database Engines">
      <div class="vdbe-view-wrap">
        {virtualInfra &&
          virtualInfra.engines &&
          virtualInfra.engines.map(({ index, ...props }) => (
            <VdbeView key={index} name={`VDBE ${index}`} {...props} />
          ))}
      </div>
    </Panel>
  );
}

export default VirtualInfraView;
