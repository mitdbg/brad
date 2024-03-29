import { useState, useEffect } from "react";
import "./styles/WorkloadAdjuster.css";
import Slider from "@mui/material/Slider";

function WorkloadAdjuster({ min, max, value, onChange, debounceMs }) {
  const [currentValue, setCurrentValue] = useState(value);

  useEffect(() => {
    const timeoutId = setTimeout(() => onChange(currentValue), debounceMs);
    return () => {
      clearTimeout(timeoutId);
    };
  }, [currentValue, debounceMs]);

  return (
    <div class="workload-adjuster">
      <h3>Number of Workload Clients ({currentValue})</h3>
      <Slider
        value={currentValue}
        marks={[
          { label: min, value: min },
          { label: max, value: max },
        ]}
        min={min}
        max={max}
        onChange={(_, newValue) => setCurrentValue(newValue)}
      />
    </div>
  );
}

export default WorkloadAdjuster;
