import { useCallback, useState, useEffect } from "react";
import Header from "./components/Header";
import VirtualInfraView from "./components/VirtualInfraView";
import BlueprintView from "./components/BlueprintView";
import PerfView from "./components/PerfView";
import WorkloadInput from "./components/WorkloadInput";
import CreateEditVdbeForm from "./components/CreateEditVdbeForm";
import StorageRoundedIcon from "@mui/icons-material/StorageRounded";
import Panel from "./components/Panel";
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
      },
      {
        host: "localhost",
        port: 8586,
      },
    ],
  });
  const [configModalOpen, setConfigModalOpen] = useState(false);
  const [workloadInputState, setWorkloadInputState] = useState({
    open: false,
    engineIntensity: [],
    min: 1,
    max: 10,
  });
  const [vdbeFormState, setVdbeFormState] = useState({
    open: false,
    currentVdbe: null,
  });

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

  const refreshData = async () => {
    const newSystemState = await fetchSystemState(
      /*filterTablesForDemo=*/ false,
    );
    // TODO: Not the best way to check for equality.
    if (JSON.stringify(systemState) !== JSON.stringify(newSystemState)) {
      setSystemState(newSystemState);
    }
  };

  // Fetch updated system state periodically.
  useEffect(() => {
    // Run first fetch immediately.
    refreshData();
    const intervalId = setInterval(refreshData, REFRESH_INTERVAL_MS);
    return () => {
      if (intervalId === null) {
        return;
      }
      clearInterval(intervalId);
    };
  }, [systemState]);

  // Bind keyboard shortcut for internal config menu.
  const handleKeyPress = useCallback(
    (event) => {
      if (document.activeElement !== document.body) {
        // We only want to handle key presses when no input is focused.
        return;
      }
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

  const allTables = [
    "tickets",
    "theatres",
    "movies",
    "showings",
    "aka_title",
    "homes",
    "movie_info",
    "title",
    "company_name",
  ];

  return (
    <>
      <Header
        status={systemState.status}
        workloadDisabled={false}
        onWorkloadClick={() => {
          if (workloadInputState.open) return;
          setWorkloadInputState({ ...workloadInputState, open: true });
        }}
      />
      <div class="body-container">
        <div class="column" style={{ flexGrow: 3 }}>
          <h2 class="col-h2">
            <StorageRoundedIcon style={{ marginRight: "8px" }} />
            Data Infrastructure
          </h2>
          <div class="column-inner">
            <Panel>
              {workloadInputState.open && (
                <WorkloadInput
                  workloadInputState={workloadInputState}
                  setWorkloadInputState={setWorkloadInputState}
                />
              )}
              {vdbeFormState.open && (
                <CreateEditVdbeForm
                  currentVdbe={vdbeFormState.currentVdbe}
                  allTables={allTables}
                  onCloseClick={() =>
                    setVdbeFormState({ open: false, currentVdbe: null })
                  }
                />
              )}
              <VirtualInfraView
                virtualInfra={systemState.virtual_infra}
                highlight={highlight}
                onTableHoverEnter={onTableHoverEnter}
                onTableHoverExit={onTableHoverExit}
                endpoints={endpoints}
                onAddVdbeClick={() => {
                  if (vdbeFormState.open) return;
                  setVdbeFormState({ open: true, currentVdbe: null });
                }}
                onEditVdbeClick={(vdbe) => {
                  if (vdbeFormState.open) return;
                  setVdbeFormState({ open: true, currentVdbe: vdbe });
                }}
              />
              <div class="infra-separator" />
              <BlueprintView
                blueprint={systemState.blueprint}
                nextBlueprint={systemState.next_blueprint}
                highlight={highlight}
                onTableHoverEnter={onTableHoverEnter}
                onTableHoverExit={onTableHoverExit}
              />
            </Panel>
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
