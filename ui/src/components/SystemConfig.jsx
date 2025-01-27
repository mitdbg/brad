import TextField from "@mui/material/TextField";
import Button from "@mui/material/Button";
import FormGroup from "@mui/material/FormGroup";
import FormControlLabel from "@mui/material/FormControlLabel";
import Switch from "@mui/material/Switch";
import Dialog from "@mui/material/Dialog";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import DialogActions from "@mui/material/DialogActions";
import "./styles/SystemConfig.css";

// Currently unused.
function EndpointInput({ name, host, port, onChange }) {
  return (
    <FormGroup className="endpoint-input">
      <TextField
        variant="outlined"
        label={`${name} Host`}
        value={host}
        onChange={(event) => onChange({ host: event.target.value, port })}
      />
      <TextField
        variant="outlined"
        label={`${name} Port`}
        value={port}
        onChange={(event) => onChange({ host, port: +event.target.value })}
      />
    </FormGroup>
  );
}

function SystemConfig({ open, onCloseClick, config, onConfigChange }) {
  const { showVdbeSpecificMetrics } = config;
  return (
    <Dialog open={open} onClose={onCloseClick}>
      <DialogTitle>Dashboard Configuration</DialogTitle>
      <DialogContent>
        <FormGroup>
          <FormControlLabel
            control={<Switch checked={showVdbeSpecificMetrics} />}
            label="Display VDBE-specific Metrics"
            onChange={(event) =>
              onConfigChange({ showVdbeSpecificMetrics: event.target.checked })
            }
          />
        </FormGroup>
      </DialogContent>
      <DialogActions>
        <Button onClick={onCloseClick}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}

export default SystemConfig;
