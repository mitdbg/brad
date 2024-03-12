import Panel from "./Panel";
import VdbeView from "./VdbeView";
import "./styles/VirtualInfraView.css";

function VirtualInfraView({
  virtualInfra,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
}) {
  return (
    <Panel heading="Virtual Database Engines">
      <div class="vdbe-view-wrap">
        {virtualInfra &&
          virtualInfra.engines &&
          virtualInfra.engines.map(({ name, ...props }) => (
            <VdbeView
              key={name}
              name={name}
              highlight={highlight}
              onTableHoverEnter={onTableHoverEnter}
              onTableHoverExit={onTableHoverExit}
              {...props}
            />
          ))}
      </div>
    </Panel>
  );
}

export default VirtualInfraView;
