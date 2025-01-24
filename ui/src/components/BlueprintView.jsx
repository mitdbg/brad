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
  previewBlueprint,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
}) {
  let blueprintToShow = blueprint;
  if (previewBlueprint != null) {
    blueprintToShow = previewBlueprint;
  }
  return (
    <div class="infra-region bp-view-wrap">
      <h2>Physical</h2>
      <div class="bp-view-engines-wrap">
        {blueprintToShow &&
          blueprintToShow.engines &&
          blueprintToShow.engines.map(({ name, ...props }) => (
            <PhysDbView
              key={name}
              name={name}
              {...props}
              highlight={highlight}
              onTableHoverEnter={onTableHoverEnter}
              onTableHoverExit={onTableHoverExit}
              // nextEngine={findNextEngine(name, nextBlueprint)}
            />
          ))}
      </div>
    </div>
  );
}

export default BlueprintView;
