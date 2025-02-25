import axios from "axios";

const API_PREFIX = "/api/1";

function flagToString(name, value) {
  return `${name}=${value ? "true" : "false"}`;
}

async function fetchMetrics(numHistoricalValues, useGenerated) {
  const result = await axios.get(
    `${API_PREFIX}/metrics?num_values=${numHistoricalValues}&${flagToString("use_generated", useGenerated)}`,
  );
  return result.data;
}

async function fetchSystemState(filterTablesForDemo) {
  const result = await axios.get(
    `${API_PREFIX}/system_state?${flagToString("filter_tables_for_demo", filterTablesForDemo)}`,
  );
  return result.data;
}

async function fetchWorkloadClients(port) {
  const args = port != null ? { params: { runner_port: port } } : {};
  const result = await axios.get(`${API_PREFIX}/clients`, args);
  return result.data;
}

async function setWorkloadClients(port, numClients) {
  const args = { curr_clients: numClients };
  if (port != null) {
    args.runner_port = port;
  }
  const result = await axios.post(`${API_PREFIX}/clients`, args);
  return result.data;
}

async function getPredictedChanges(tMultiplier, aMultiplier) {
  const result = await axios.post(`${API_PREFIX}/predicted_changes`, {
    t_multiplier: tMultiplier,
    a_multiplier: aMultiplier,
  });
  return result.data;
}

async function createVdbe(vdbe) {
  const result = await axios.post(`${API_PREFIX}/vdbe`, vdbe);
  return result.data;
}

async function updateVdbe(vdbe) {
  const result = await axios.put(`${API_PREFIX}/vdbe`, vdbe);
  return result.data;
}

async function deleteVdbe(vdbeId) {
  const result = await axios.delete(`${API_PREFIX}/vdbe/${vdbeId}`);
  return result.data;
}

export {
  fetchMetrics,
  fetchSystemState,
  fetchWorkloadClients,
  setWorkloadClients,
  getPredictedChanges,
  createVdbe,
  updateVdbe,
  deleteVdbe,
};
