import PhysDbView from "./PhysDbView";
import Chip from "@mui/material/Chip";
import AutoAwesomeRoundedIcon from "@mui/icons-material/AutoAwesomeRounded";
import HighlightablePhysDb from "./HighlightablePhysDb";
import "./styles/BlueprintView.css";

function findNextEngine(engineKind, nextBlueprint) {
  if (nextBlueprint == null) return null;
  for (const engine of nextBlueprint.engines) {
    if (engine.engine === engineKind) {
      return engine;
    }
  }
  return null;
}

function ShowingPreviewIndicator() {
  return (
    <div className="bp-preview-indicator">
      <Chip
        color="primary"
        icon={<AutoAwesomeRoundedIcon />}
        label="Showing Predicted Changes"
      />
    </div>
  );
}

function PreviewBlueprint({ blueprint }) {
  return (
    <>
      <div className="bp-preview-indicator-wrap">
        <ShowingPreviewIndicator />
      </div>
      <div class="bp-view-engines-wrap">
        {blueprint &&
          blueprint.engines &&
          blueprint.engines.map(({ name, ...props }) => (
            <PhysDbView key={name} name={name} {...props} />
          ))}
      </div>
    </>
  );
}

function CurrentBlueprint({ blueprint, nextBlueprint }) {
  return (
    <div class="bp-view-engines-wrap">
      {blueprint &&
        blueprint.engines &&
        blueprint.engines.map(({ engine, mapped_vdbes, ...props }) => (
          <HighlightablePhysDb
            key={engine}
            engine={engine}
            mappedVdbes={mapped_vdbes}
            {...props}
            nextEngine={findNextEngine(engine, nextBlueprint)}
          />
        ))}
    </div>
  );
}

function BlueprintView({ blueprint, nextBlueprint, previewBlueprint }) {
  return (
    <div class="infra-region bp-view-wrap">
      <h2>Physical</h2>
      {previewBlueprint != null ? (
        <PreviewBlueprint blueprint={previewBlueprint} />
      ) : (
        <CurrentBlueprint blueprint={blueprint} nextBlueprint={nextBlueprint} />
      )}
    </div>
  );
}

export default BlueprintView;
