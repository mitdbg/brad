import TextField from "@mui/material/TextField";
import Modal from "@mui/material/Modal";
import Button from '@mui/material/Button';
import "./styles/SystemConfig.css";

function EndpointInput({ name, host, port }) {
  return (
    <div class="endpoint-input">
      <TextField variant="outlined" label={`${name} Host`} value={host} />
      <TextField variant="outlined" label={`${name} Port`} value={port} />
    </div>
  );
}

function SystemConfig({ endpoints }) {
  const { brad, workloadRunners } = endpoints;
  return (
    <div class="system-config-modal">
      <h2>Dashboard Configuration</h2>
      <EndpointInput name="BRAD" {...brad} />
      {workloadRunners.map((endpoint, index) => (
        <EndpointInput
          key={`${endpoint.host}:${endpoint.port}`}
          name={`Runner ${index + 1}`}
          {...endpoint}
        />
      ))}
      <Button variant="contained">Ok</Button>
    </div>
  );
}

export default SystemConfig;
