import { useState } from "react";
import InsetPanel from "./InsetPanel";
import CheckCircleOutlineRoundedIcon from "@mui/icons-material/CheckCircleOutlineRounded";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import TextField from "@mui/material/TextField";
import MenuItem from "@mui/material/MenuItem";
import InputAdornment from "@mui/material/InputAdornment";
import Button from "@mui/material/Button";
import AddCircleOutlineIcon from "@mui/icons-material/AddCircleOutline";
import EditRoundedIcon from "@mui/icons-material/EditRounded";
import Select from "@mui/material/Select";
import VdbeView from "./VdbeView";
import "./styles/CreateEditVdbeForm.css";

function CreateEditFormFields({ vdbe, setVdbe }) {
  const onStalenessChange = (event) => {
    const maxStalenessMins = parseInt(event.target.value);
    if (isNaN(maxStalenessMins)) {
      setVdbe({ ...vdbe, max_staleness_ms: null });
      return;
    }
    setVdbe({ ...vdbe, max_staleness_ms: maxStalenessMins * 60 * 1000 });
  };

  const onSloChange = (event) => {
    const sloMs = parseInt(event.target.value);
    if (isNaN(sloMs)) {
      setVdbe({ ...vdbe, p90_latency_slo_ms: null });
      return;
    }
    setVdbe({ ...vdbe, p90_latency_slo_ms: sloMs });
  };

  return (
    <div className="cev-form-fields">
      <TextField
        variant="outlined"
        label="Name"
        className="cev-field"
        value={vdbe.name}
        onChange={(event) => setVdbe({ ...vdbe, name: event.target.value })}
      />
      <TextField
        variant="outlined"
        label="Maximum Staleness (Freshness Constraint)"
        className="cev-field"
        slotProps={{
          input: {
            endAdornment: (
              <InputAdornment position="end">minutes</InputAdornment>
            ),
          },
        }}
        value={
          vdbe.max_staleness_ms != null ? vdbe.max_staleness_ms / 60000 : ""
        }
        onChange={onStalenessChange}
      />
      <TextField
        variant="outlined"
        label="Maximum p90 Latency (Performance SLO)"
        className="cev-field"
        slotProps={{
          input: {
            endAdornment: (
              <InputAdornment position="end">milliseconds</InputAdornment>
            ),
          },
        }}
        value={vdbe.p90_latency_slo_ms != null ? vdbe.p90_latency_slo_ms : ""}
        onChange={onSloChange}
      />
      <FormControl fullWidth>
        <InputLabel id="cev-query-interface">Query Interface</InputLabel>
        <Select
          labelId="cev-query-interface"
          variant="outlined"
          label="Query Interface"
          className="cev-field"
          value={vdbe.interface}
          onChange={(event) =>
            setVdbe({ ...vdbe, interface: event.target.value })
          }
        >
          <MenuItem value="common">Common SQL</MenuItem>
          <MenuItem value="postgresql">PostgreSQL SQL</MenuItem>
          <MenuItem value="athena">Athena SQL</MenuItem>
        </Select>
      </FormControl>
    </div>
  );
}

function getEmptyVdbe() {
  return {
    name: null,
    max_staleness_ms: null,
    p90_latency_slo_ms: null,
    queryInterface: "postgresql",
    tables: [],
  };
}

function CreateEditVdbeForm({ isEdit, currentVdbe }) {
  const [vdbe, setVdbe] = useState(
    currentVdbe != null ? currentVdbe : getEmptyVdbe(),
  );

  return (
    <InsetPanel className="create-edit-vdbe-form">
      <h2>
        {isEdit ? (
          <EditRoundedIcon style={{ marginRight: "10px" }} />
        ) : (
          <AddCircleOutlineIcon style={{ marginRight: "10px" }} />
        )}
        {isEdit ? "Edit VDBE" : "Create VDBE"}
      </h2>
      <div className="cev-form-body">
        <CreateEditFormFields vdbe={vdbe} setVdbe={setVdbe} />
        <div className="cev-preview">
          <VdbeView vdbe={vdbe} highlight={{}} editable={false} />
        </div>
      </div>
      <div className="cev-buttons">
        <Button variant="outlined">Cancel</Button>
        <Button
          variant="contained"
          startIcon={<CheckCircleOutlineRoundedIcon />}
        >
          {isEdit ? "Save" : "Create"}
        </Button>
      </div>
    </InsetPanel>
  );
}

export default CreateEditVdbeForm;
