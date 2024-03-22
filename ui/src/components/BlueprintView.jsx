import Panel from "./Panel";
import PhysDbView from "./PhysDbView";
import "./styles/BlueprintView.css";

function BlueprintView({
  blueprint,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
}) {
  return (
    <Panel
      heading="Physical Infrastructure"
      className="infra-column-panel"
    >
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
            />
          ))}
      </div>
    </Panel>
  );
}

export default BlueprintView;
