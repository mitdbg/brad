import { useCallback, useState, useEffect } from "react";
import Header from "./components/Header";
import PerfView from "./components/PerfView";
import OverallInfraView from "./components/OverallInfraView";
import { fetchSystemState } from "./api";

import "./App.css";

const REFRESH_INTERVAL_MS = 30 * 1000;

function App() {
  const [appState, setAppState] = useState({
    systemState: {
      status: "running",
      blueprint: null,
      virtual_infra: null,
      next_blueprint: null,
    },
    workloadInputOpen: false,
    vdbeForm: {
      open: false,
      shownVdbe: null,
    },
  });

  const refreshData = useCallback(async () => {
    const newSystemState = await fetchSystemState();
    // TODO: Not the best way to check for equality.
    if (
      JSON.stringify(appState.systemState) !== JSON.stringify(newSystemState)
    ) {
      setAppState({ ...appState, systemState: newSystemState });
    }
  }, [appState, setAppState]);

  // Fetch updated system state periodically.
  useEffect(() => {
    refreshData();
    const intervalId = setInterval(refreshData, REFRESH_INTERVAL_MS);
    return () => {
      if (intervalId === null) {
        return;
      }
      clearInterval(intervalId);
    };
  }, [refreshData]);

  // Bind keyboard shortcut for internal config menu.
  const handleKeyPress = useCallback((event) => {
    if (document.activeElement !== document.body) {
      // We only want to handle key presses when no input is focused.
      return;
    }
    // Currently a no-op.
  }, []);

  useEffect(() => {
    document.addEventListener("keyup", handleKeyPress);
    return () => {
      document.removeEventListener("keyup", handleKeyPress);
    };
  }, [handleKeyPress]);

  const { systemState, workloadInputOpen, vdbeForm } = appState;

  // Callbacks used to control forms in the UI.
  const openWorkloadInput = () => {
    if (workloadInputOpen) return;
    setAppState({ ...appState, workloadInputOpen: true });
  };
  const closeWorkloadInput = () => {
    if (!workloadInputOpen) return;
    setAppState({ ...appState, workloadInputOpen: false });
  };

  const openVdbeForm = (vdbe) => {
    const { open } = vdbeForm;
    if (open) return;
    setAppState({ ...appState, vdbeForm: { open: true, shownVdbe: vdbe } });
  };
  const closeVdbeForm = () => {
    const { open } = vdbeForm;
    if (!open) return;
    setAppState({ ...appState, vdbeForm: { open: false, shownVdbe: null } });
  };

  return (
    <>
      <Header
        status={systemState.status}
        workloadDisabled={workloadInputOpen || vdbeForm.open}
        onWorkloadClick={openWorkloadInput}
      />
      <div class="body-container">
        <OverallInfraView
          appState={appState}
          closeWorkloadInput={closeWorkloadInput}
          openVdbeForm={openVdbeForm}
          closeVdbeForm={closeVdbeForm}
        />
        <PerfView virtualInfra={systemState.virtual_infra} />
      </div>
    </>
  );
}

export default App;
