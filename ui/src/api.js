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
  const args = port != null ? {runner_port: port} : {};
  const result = await axios.get(`${API_PREFIX}/clients`, args);
  return result.data;
}

async function setWorkloadClients(port, numClients) {
  const args = {curr_clients: numClients};
  if (port != null) {
    args.runner_port = port;
  }
  const result = await axios.post(`${API_PREFIX}/clients`, args);
  return result.data;
}

export { fetchMetrics, fetchSystemState, fetchWorkloadClients, setWorkloadClients };
