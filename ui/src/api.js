import axios from "axios";

const API_PREFIX = "/api/1";

async function fetchMetrics(numHistoricalValues) {
  const result = await axios.get(
    `${API_PREFIX}/metrics?num_values=${numHistoricalValues}`,
  );
  return result.data;
}

async function fetchSystemState() {
  const result = await axios.get(`${API_PREFIX}/system_state`);
  return result.data;
}

export { fetchMetrics, fetchSystemState };
