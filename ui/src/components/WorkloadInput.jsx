import Button from "@mui/material/Button";
import Slider from "@mui/material/Slider";
import InsetPanel from "./InsetPanel";
import AutoFixHighRoundedIcon from "@mui/icons-material/AutoFixHighRounded";
import TuneRoundedIcon from "@mui/icons-material/TuneRounded";
import HelpRoundedIcon from "@mui/icons-material/HelpRounded";
import Tooltip from "@mui/material/Tooltip";
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

function WorkloadInput({ workloadInputState, setWorkloadInputState }) {
  const { min, max, engineIntensity } = workloadInputState;

  const handleClose = () => {
    setWorkloadInputState({
      ...workloadInputState,
      engineIntensity: [],
      open: false,
    });
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
        <WorkloadSlider engineName="VDBE (A)" min={min} max={max} value={3} />
        <WorkloadSlider engineName="VDBE (B)" min={min} max={max} value={5} />
        <WorkloadSlider engineName="VDBE (C)" min={min} max={max} value={5} />
      </div>
      <div className="workload-input-buttons">
        <Button variant="outlined" onClick={handleClose}>
          Reset and Close
        </Button>
        <Button variant="contained" startIcon={<AutoFixHighRoundedIcon />}>
          Show Predicted Changes
        </Button>
      </div>
    </InsetPanel>
  );
}

export default WorkloadInput;
