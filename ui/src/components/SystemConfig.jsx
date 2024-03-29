import TextField from "@mui/material/TextField";
import Modal from "@mui/material/Modal";
import Button from "@mui/material/Button";
import "./styles/SystemConfig.css";

function EndpointInput({ name, host, port, onChange }) {
  return (
    <div class="endpoint-input">
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
    </div>
  );
}

function SystemConfig({ endpoints, open, onCloseClick, onChange }) {
  const { workloadRunners } = endpoints;
  return (
    <Modal open={open}>
      <div class="system-config-modal">
        <h2>Dashboard Configuration</h2>
        {workloadRunners.map((endpoint, index) => (
          <EndpointInput
            key={index}
            name={`Runner ${index + 1}`}
            {...endpoint}
            onChange={(newEndpoint) =>
              onChange({
                field: "workloadRunners",
                value: workloadRunners.map((innerEndpoint, innerIndex) =>
                  innerIndex === index ? newEndpoint : innerEndpoint,
                ),
              })
            }
          />
        ))}
        <Button variant="contained" onClick={onCloseClick}>
          Close
        </Button>
      </div>
    </Modal>
  );
}

export default SystemConfig;
