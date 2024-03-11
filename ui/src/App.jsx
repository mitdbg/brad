import { useState, useEffect } from "react";
import Header from "./components/Header";
import VirtualInfraView from "./components/VirtualInfraView";
import BlueprintView from "./components/BlueprintView";
import PerfView from "./components/PerfView";
import { fetchSystemState } from "./api";

import "./App.css";

const REFRESH_INTERVAL_MS = 30 * 1000;

function App() {
  const [systemState, setSystemState] = useState({});

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
        <div class="column">
          <h2 class="col-h2">Data Infrastructure</h2>
          <div class="column-inner">
            <VirtualInfraView systemState={systemState} />
            <BlueprintView systemState={systemState} />
          </div>
        </div>
        <div class="column">
          <h2 class="col-h2">Performance Monitoring</h2>
          <div class="column-inner">
            <PerfView />
          </div>
        </div>
      </div>
    </>
  );
}

export default App;
