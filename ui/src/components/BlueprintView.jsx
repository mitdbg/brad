import Panel from "./Panel";
import PhysDbView from "./PhysDbView";
import "./styles/BlueprintView.css";

function BlueprintView({ systemState }) {
  return (
    <Panel heading="Current Blueprint (Physical Infrastructure)">
      <div class="bp-view-wrap">
        {systemState &&
          systemState.engines &&
          systemState.engines.map(({ name, ...props }) => (
            <PhysDbView key={name} name={name} {...props} />
          ))}
      </div>
    </Panel>
  );
}

export default BlueprintView;
