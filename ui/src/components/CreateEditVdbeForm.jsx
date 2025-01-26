import { useState } from "react";
import { useTheme } from "@mui/material/styles";
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
import FormHelperText from "@mui/material/FormHelperText";
import Chip from "@mui/material/Chip";
import Box from "@mui/material/Box";
import OutlinedInput from "@mui/material/OutlinedInput";
import Radio from "@mui/material/Radio";
import RadioGroup from "@mui/material/RadioGroup";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormLabel from "@mui/material/FormLabel";
import VdbeView from "./VdbeView";
import { createVdbe, updateVdbe } from "../api";
import "./styles/CreateEditVdbeForm.css";

const ITEM_HEIGHT = 47;
const ITEM_PADDING_TOP = 7;
const MenuProps = {
  PaperProps: {
    style: {
      maxHeight: ITEM_HEIGHT * 3.5 + ITEM_PADDING_TOP,
      width: 249,
    },
  },
};

function getStyles(name, selectedTables, theme) {
  return {
    fontWeight: selectedTables.includes(name)
      ? theme.typography.fontWeightMedium
      : theme.typography.fontWeightRegular,
  };
}

function TableSelector({ selectedTables, setSelectedTables, allTables }) {
  const theme = useTheme();

  const handleChange = (event) => {
    const {
      target: { value },
    } = event;
    setSelectedTables(
      // On autofill we get a stringified value.
      typeof value === "string" ? value.split(",") : value,
    );
  };

  return (
    <div>
      <FormControl fullWidth>
        <InputLabel id="cev-table-selector-label">Tables</InputLabel>
        <Select
          labelId="cev-table-selector-label"
          label="Tables"
          id="cev-table-selector"
          multiple
          value={selectedTables}
          onChange={handleChange}
          input={<OutlinedInput id="cev-table-selector-field" label="Tables" />}
          renderValue={(selected) => (
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: "10px 10px" }}>
              {selected.map((value) => (
                <Chip key={value} label={value} />
              ))}
            </Box>
          )}
          MenuProps={MenuProps}
        >
          {allTables.map((name) => (
            <MenuItem
              key={name}
              value={name}
              style={getStyles(name, selectedTables, theme)}
            >
              {name}
            </MenuItem>
          ))}
        </Select>
        <FormHelperText>
          Click the tables in the preview to toggle their write flags.
        </FormHelperText>
      </FormControl>
    </div>
  );
}

function CreateEditFormFields({ vdbe, setVdbe, allTables, validEngines }) {
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

  const setSelectedTables = (tables) => {
    const nextTables = [];
    for (const table of tables) {
      let existingTable = vdbe.tables.find(({ name }) => name === table);
      if (existingTable != null) {
        nextTables.push(existingTable);
      } else {
        nextTables.push({ name: table, writable: false });
      }
    }
    setVdbe({ ...vdbe, tables: nextTables });
  };

  const onMappedToChange = (event) => {
    setVdbe({ ...vdbe, mapped_to: event.target.value });
  };

  const mappedToEngine = vdbe.mapped_to;

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
        <TableSelector
          selectedTables={vdbe.tables.map(({ name }) => name)}
          setSelectedTables={setSelectedTables}
          allTables={allTables}
        />
      </FormControl>
      <FormControl fullWidth style={{ marginTop: "15px" }}>
        <FormLabel id="map-vdbe-label">Map VDBE To</FormLabel>
        <RadioGroup row name="position" defaultValue="top">
          <FormControlLabel
            value="aurora"
            control={<Radio />}
            label="Aurora"
            labelPlacement="end"
            checked={mappedToEngine === "aurora"}
            onClick={onMappedToChange}
            disabled={!validEngines.includes("aurora")}
          />
          <FormControlLabel
            value="redshift"
            control={<Radio style={{ marginLeft: "8px" }} />}
            label="Redshift"
            labelPlacement="end"
            checked={mappedToEngine === "redshift"}
            onClick={onMappedToChange}
            disabled={!validEngines.includes("redshift")}
          />
          <FormControlLabel
            value="athena"
            control={<Radio style={{ marginLeft: "8px" }} />}
            label="Athena"
            labelPlacement="end"
            checked={mappedToEngine === "athena"}
            onClick={onMappedToChange}
            disabled={!validEngines.includes("athena")}
          />
        </RadioGroup>
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

function vdbesEqual(vdbe1, vdbe2) {
  if (vdbe1 == null || vdbe2 == null) {
    return false;
  }
  if (
    !(
      vdbe1.name === vdbe2.name &&
      vdbe1.max_staleness_ms === vdbe2.max_staleness_ms &&
      vdbe1.p90_latency_slo_ms === vdbe2.p90_latency_slo_ms &&
      vdbe1.interface === vdbe2.interface &&
      vdbe1.tables.length === vdbe2.tables.length &&
      vdbe1.mapped_to === vdbe2.mapped_to
    )
  ) {
    return false;
  }

  // Check for table equality without regard to order.
  return vdbe1.tables.every(({ name, writable }) => {
    const matching = vdbe2.tables.find(
      (table2) => table2.name === name && table2.writable === writable,
    );
    return matching != null;
  });
}

function isValid(vdbe) {
  return (
    vdbe.name != null &&
    vdbe.max_staleness_ms != null &&
    vdbe.max_staleness_ms >= 0 &&
    vdbe.p90_latency_slo_ms != null &&
    vdbe.p90_latency_slo_ms > 0 &&
    vdbe.interface != null &&
    vdbe.tables.length > 0 &&
    vdbe.mapped_to != null
  );
}

function validEngines(blueprint) {
  if (blueprint == null) {
    return [];
  }
  return blueprint.engines.map((engine) => engine.engine);
}

function CreateEditVdbeForm({
  currentVdbe,
  blueprint,
  allTables,
  onCloseClick,
  onVdbeChangeSuccess,
}) {
  const isEdit = currentVdbe != null;
  const [vdbe, setVdbe] = useState(
    currentVdbe != null ? currentVdbe : getEmptyVdbe(),
  );
  const [inFlight, setInFlight] = useState(false);
  const hasChanges = currentVdbe == null || !vdbesEqual(currentVdbe, vdbe);

  const onTableClick = (tableName) => {
    const nextTables = [];
    for (const table of vdbe.tables) {
      if (table.name === tableName) {
        nextTables.push({ ...table, writable: !table.writable });
      } else {
        nextTables.push(table);
      }
    }
    setVdbe({ ...vdbe, tables: nextTables });
  };

  const onSaveClick = async () => {
    setInFlight(true);
    if (isEdit) {
      await updateVdbe(vdbe);
    } else {
      await createVdbe(vdbe);
    }
    await onVdbeChangeSuccess();
    setInFlight(false);
    onCloseClick();
  };

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
        <CreateEditFormFields
          vdbe={vdbe}
          setVdbe={setVdbe}
          allTables={allTables}
          validEngines={validEngines(blueprint)}
        />
        <div className="cev-preview">
          <div className="cev-preview-label">
            <Chip label="Preview" variant="outlined" />
          </div>
          <VdbeView
            vdbe={vdbe}
            highlight={{}}
            editable={false}
            onTableClick={onTableClick}
            hideEndpoint={true}
          />
        </div>
      </div>
      <div className="cev-buttons">
        <Button variant="outlined" onClick={onCloseClick} disabled={inFlight}>
          Cancel
        </Button>
        <Button
          variant="contained"
          startIcon={<CheckCircleOutlineRoundedIcon />}
          disabled={!hasChanges || !isValid(vdbe)}
          loading={inFlight}
          onClick={onSaveClick}
        >
          {isEdit ? "Save" : "Create"}
        </Button>
      </div>
    </InsetPanel>
  );
}

export default CreateEditVdbeForm;
