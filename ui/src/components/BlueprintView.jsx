import Panel from "./Panel";
import PhysDbView from "./PhysDbView";
import "./styles/BlueprintView.css";

function findNextEngine(engineName, nextBlueprint) {
  if (nextBlueprint == null) return null;
  for (const engine of nextBlueprint.engines) {
    if (engine.name === engineName) {
      return engine;
    }
  }
  return null;
}

function BlueprintView({
  blueprint,
  nextBlueprint,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
}) {
  return (
    <Panel heading="Physical Infrastructure" className="infra-column-panel">
      <div class="bp-view-wrap">
        {blueprint &&
          blueprint.engines &&
          blueprint.engines.map(({ name, ...props }) => (
            <PhysDbView
              key={name}
              name={name}
              {...props}
              highlight={highlight}
              onTableHoverEnter={onTableHoverEnter}
              onTableHoverExit={onTableHoverExit}
              nextEngine={findNextEngine(name, nextBlueprint)}
            />
          ))}
      </div>
    </Panel>
  );
}

export default BlueprintView;
