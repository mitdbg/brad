import { useCallback, useState, useEffect } from "react";
import Header from "./components/Header";
import VirtualInfraView from "./components/VirtualInfraView";
import BlueprintView from "./components/BlueprintView";
import PerfView from "./components/PerfView";
import SystemConfig from "./components/SystemConfig";
import { fetchSystemState } from "./api";

import "./App.css";

const REFRESH_INTERVAL_MS = 30 * 1000;

function App() {
  const [systemState, setSystemState] = useState({
    status: "running",
    blueprint: null,
    virtual_infra: null,
    next_blueprint: null,
  });
  const [highlight, setHighlight] = useState({
    hoverEngine: null,
    virtualEngines: {},
    physicalEngines: {},
  });
  const [endpoints, setEndpoints] = useState({
    workloadRunners: [
      {
        host: "localhost",
        port: 8585,
        // port: 7583,
      },
      {
        host: "localhost",
        port: 8586,
        // port: 7583,
      },
    ],
  });
  const [configModalOpen, setConfigModalOpen] = useState(false);

  const onTableHoverEnter = (engineMarker, tableName, isVirtual, mappedTo) => {
    const virtualEngines = {};
    const physicalEngines = {};
    if (isVirtual) {
      virtualEngines[engineMarker] = tableName;
      for (const physMarker of mappedTo) {
        physicalEngines[physMarker] = tableName;
      }
    } else {
      physicalEngines[engineMarker] = tableName;
      for (const virtMarker of mappedTo) {
        virtualEngines[virtMarker] = tableName;
      }
    }
    setHighlight({
      hoverEngine: engineMarker,
      virtualEngines,
      physicalEngines,
    });
  };

  const onTableHoverExit = () => {
    setHighlight({
      hoverEngine: null,
      virtualEngines: {},
      physicalEngines: {},
    });
  };

  // Fetch updated system state periodically.
  useEffect(() => {
    let timeoutId = null;
    const refreshData = async () => {
      const newSystemState = await fetchSystemState(
        /*filterTablesForDemo=*/ true,
      );
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

  // Bind keyboard shortcut for internal config menu.
  const handleKeyPress = useCallback(
    (event) => {
      if (event.key === "d" && !configModalOpen) {
        setConfigModalOpen(true);
      }
    },
    [configModalOpen],
  );

  useEffect(() => {
    document.addEventListener("keyup", handleKeyPress);
    return () => {
      document.removeEventListener("keyup", handleKeyPress);
    };
  }, [handleKeyPress]);

  const handleSystemConfigChange = useCallback(
    ({ field, value }) => {
      setEndpoints({ ...endpoints, [field]: value });
    },
    [endpoints],
  );

  return (
    <>
      <Header status={systemState.status} />
      <div class="body-container">
        <div class="column" style={{ flexGrow: 3 }}>
          <h2 class="col-h2">Data Infrastructure</h2>
          <div class="column-inner">
            <VirtualInfraView
              virtualInfra={systemState.virtual_infra}
              highlight={highlight}
              onTableHoverEnter={onTableHoverEnter}
              onTableHoverExit={onTableHoverExit}
              endpoints={endpoints}
            />
            <BlueprintView
              blueprint={systemState.blueprint}
              highlight={highlight}
              onTableHoverEnter={onTableHoverEnter}
              onTableHoverExit={onTableHoverExit}
            />
          </div>
        </div>
        <PerfView virtualInfra={systemState.virtual_infra} />
        <SystemConfig
          endpoints={endpoints}
          open={configModalOpen}
          onCloseClick={() => setConfigModalOpen(false)}
          onChange={handleSystemConfigChange}
        />
      </div>
    </>
  );
}

export default App;
