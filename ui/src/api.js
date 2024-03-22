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

export { fetchMetrics, fetchSystemState };
