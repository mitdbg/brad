import { useCallback, useState, useEffect } from "react";
import Header from "./components/Header";
import PerfView from "./components/PerfView";
import OverallInfraView from "./components/OverallInfraView";
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
    all_tables: [],
  });
  const [appState, setAppState] = useState({
    previewForm: {
      open: false,
      shownPreviewBlueprint: null,
    },
    vdbeForm: {
      open: false,
      shownVdbe: null,
    },
  });
  const [config, setConfig] = useState({
    showVdbeSpecificMetrics: true,
  });
  const [showConfigModal, setShowConfigModal] = useState(false);

  // Used for system state refresh.
  const refreshSystemState = useCallback(async () => {
    const newSystemState = await fetchSystemState();
    // Not the best way to check for equality.
    if (JSON.stringify(systemState) !== JSON.stringify(newSystemState)) {
      setSystemState(newSystemState);
    }
  }, [systemState, setSystemState]);

  // Periodically refresh system state.
  useEffect(() => {
    refreshSystemState();
    const intervalId = setInterval(refreshSystemState, REFRESH_INTERVAL_MS);
    return () => {
      if (intervalId === null) {
        return;
      }
      clearInterval(intervalId);
    };
  }, [refreshSystemState]);

  // Bind keyboard shortcut for internal config menu.
  const handleKeyPress = useCallback(
    (event) => {
      if (document.activeElement !== document.body) {
        // We only want to handle key presses when no input is focused.
        return;
      }
      if (event.key === "c") {
        setShowConfigModal(true);
      }
    },
    [setShowConfigModal],
  );

  useEffect(() => {
    document.addEventListener("keyup", handleKeyPress);
    return () => {
      document.removeEventListener("keyup", handleKeyPress);
    };
  }, [handleKeyPress]);

  const { previewForm, vdbeForm } = appState;

  // Callbacks used to control forms in the UI.
  const openPreviewForm = () => {
    if (previewForm.open) return;
    setAppState({
      ...appState,
      previewForm: { ...previewForm, open: true },
    });
    setTimeout(() => {
      window.scrollTo({ top: 0, left: 0, behavior: "smooth" });
    }, 0);
  };
  const closePreviewForm = () => {
    if (!previewForm.open) return;
    setAppState({
      ...appState,
      previewForm: { open: false, shownPreviewBlueprint: null },
    });
  };
  const setPreviewBlueprint = (blueprint) => {
    setAppState({
      ...appState,
      previewForm: { open: true, shownPreviewBlueprint: blueprint },
    });
  };

  const openVdbeForm = (vdbe) => {
    const { open } = vdbeForm;
    if (open) return;
    setAppState({ ...appState, vdbeForm: { open: true, shownVdbe: vdbe } });
    setTimeout(() => {
      window.scrollTo({ top: 0, left: 0, behavior: "smooth" });
    }, 0);
  };
  const closeVdbeForm = () => {
    const { open } = vdbeForm;
    if (!open) return;
    setAppState({ ...appState, vdbeForm: { open: false, shownVdbe: null } });
  };

  const changeConfig = useCallback(
    (changes) => {
      setConfig({ ...config, ...changes });
    },
    [config, setConfig],
  );

  return (
    <>
      <Header
        status={systemState.status}
        workloadDisabled={previewForm.open || vdbeForm.open}
        onWorkloadClick={openPreviewForm}
      />
      <div class="body-container">
        <OverallInfraView
          systemState={systemState}
          appState={appState}
          closePreviewForm={closePreviewForm}
          openVdbeForm={openVdbeForm}
          closeVdbeForm={closeVdbeForm}
          setPreviewBlueprint={setPreviewBlueprint}
          refreshData={refreshSystemState}
        />
        <PerfView
          virtualInfra={systemState.virtual_infra}
          showingPreview={previewForm.shownPreviewBlueprint != null}
          showVdbeSpecificMetrics={config.showVdbeSpecificMetrics}
        />
      </div>
      <SystemConfig
        open={showConfigModal}
        onCloseClick={() => setShowConfigModal(false)}
        config={config}
        onConfigChange={changeConfig}
      />
    </>
  );
}

export default App;
