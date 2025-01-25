import { useState } from "react";
import Button from "@mui/material/Button";
import Slider from "@mui/material/Slider";
import InsetPanel from "./InsetPanel";
import AutoFixHighRoundedIcon from "@mui/icons-material/AutoFixHighRounded";
import TuneRoundedIcon from "@mui/icons-material/TuneRounded";
import HelpRoundedIcon from "@mui/icons-material/HelpRounded";
import Tooltip from "@mui/material/Tooltip";
import { getPredictedChanges } from "../api";
import "./styles/WorkloadInput.css";

function WorkloadSlider({ engineName, min, max, value, setValue }) {
  return (
    <div class="workload-slider">
      <h3>Workload Intensity on {engineName}</h3>
      <Slider
        value={value}
        marks={[
          { label: min, value: min },
          { label: max, value: max },
        ]}
        min={min}
        max={max}
        onChange={(_, newValue) => setValue(newValue)}
      />
    </div>
  );
}

function WorkloadInput({
  initialEngineIntensities,
  min,
  max,
  onClose,
  setPreviewBlueprint,
}) {
  const [loading, setLoading] = useState(false);
  const [engineIntensities, setEngineIntensities] = useState(
    initialEngineIntensities,
  );
  const setIntensity = (index, newValue) => {
    setEngineIntensities(
      engineIntensities.map((intensity, i) =>
        i === index ? { ...intensity, intensity: newValue } : intensity,
      ),
    );
  };
  let hasChanges = false;
  for (let i = 0; i < engineIntensities.length; i++) {
    if (
      engineIntensities[i].intensity !== initialEngineIntensities[i].intensity
    ) {
      hasChanges = true;
      break;
    }
  }

  const triggerGetPredictedChanges = async () => {
    try {
      setLoading(true);
      let tMultiplier = 1.0;
      let aMultiplier = 1.0;
      if (engineIntensities.length >= 1) {
        tMultiplier = ((engineIntensities[0].intensity - 1) / 10) * 10;
      }
      if (engineIntensities.length >= 2) {
        aMultiplier = ((engineIntensities[1].intensity - 1) / 10) * 300;
      }
      tMultiplier = Math.min(tMultiplier, 1.0);
      aMultiplier = Math.min(aMultiplier, 1.0);
      const blueprint = await getPredictedChanges(tMultiplier, aMultiplier);
      setPreviewBlueprint(blueprint);
    } catch (error) {
      console.error("Error fetching predicted changes:", error);
    } finally {
      setLoading(false);
    }
  };

  const instructions =
    "Use the sliders to change the workload intensity for each VDBE (number " +
    "of clients accessing each VDBE). Then, click 'Show Predicted Changes' to " +
    "see BRAD's predictions for how the physical infrastructure will change.";
  return (
    <InsetPanel className="workload-input-wrap">
      <h2>
        <TuneRoundedIcon style={{ marginRight: "10px" }} />
        Predict the Impact of Workload Intensity Changes
        <Tooltip title={instructions} placement="right">
          <HelpRoundedIcon
            style={{ marginLeft: "10px", opacity: "0.25", cursor: "pointer" }}
            fontSize="small"
          />
        </Tooltip>
      </h2>
      <div className="workload-input-sliders">
        {engineIntensities.map(({ name, intensity }, index) => (
          <WorkloadSlider
            key={name}
            engineName={name}
            min={min}
            max={max}
            value={intensity}
            setValue={(newValue) => setIntensity(index, newValue)}
          />
        ))}
      </div>
      <div className="workload-input-buttons">
        <Button variant="outlined" onClick={onClose} disabled={loading}>
          Reset and Close
        </Button>
        <Button
          variant="contained"
          startIcon={<AutoFixHighRoundedIcon />}
          disabled={!hasChanges || loading}
          onClick={triggerGetPredictedChanges}
          loading={loading}
        >
          Show Predicted Changes
        </Button>
      </div>
    </InsetPanel>
  );
}

export default WorkloadInput;
