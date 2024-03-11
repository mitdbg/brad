import { useState, useEffect } from "react";
import Header from "./components/Header";
import VirtualInfraView from "./components/VirtualInfraView";
import BlueprintView from "./components/BlueprintView";
import PerfView from "./components/PerfView";
import { fetchSystemState } from "./api";

import "./App.css";

const REFRESH_INTERVAL_MS = 30 * 1000;

function App() {
  const [systemState, setSystemState] = useState({
    blueprint: null,
    virtual_infra: null,
  });

  // Fetch updated system state periodically.
  useEffect(() => {
    let timeoutId = null;
    const refreshData = async () => {
      const newSystemState = await fetchSystemState();
      // TODO: Not the best way to check for equality.
      if (JSON.stringify(systemState) !== JSON.stringify(newSystemState)) {
        setSystemState(newSystemState);
      }
      timeoutId = setTimeout(refreshData, REFRESH_INTERVAL_MS);
    };

    // Run first fetch immediately.
    timeoutId = setTimeout(refreshData, 0);
    return () => {
      if (timeoutId === null) {
        return;
      }
      clearTimeout(timeoutId);
    };
  }, [systemState]);

  return (
    <>
      <Header />
      <div class="body-container">
        <div class="column" style={{ flexGrow: 3 }}>
          <h2 class="col-h2">Data Infrastructure</h2>
          <div class="column-inner">
            <VirtualInfraView virtualInfra={systemState.virtual_infra} />
            <BlueprintView blueprint={systemState.blueprint} />
          </div>
        </div>
        <PerfView virtualInfra={systemState.virtual_infra} />
      </div>
    </>
  );
}

export default App;
