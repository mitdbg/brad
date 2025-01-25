import { useCallback, useState } from "react";
import VirtualInfraView from "./VirtualInfraView";
import BlueprintView from "./BlueprintView";
import WorkloadInput from "./WorkloadInput";
import CreateEditVdbeForm from "./CreateEditVdbeForm";
import StorageRoundedIcon from "@mui/icons-material/StorageRounded";
import HighlightContext from "./HighlightContext";
import Panel from "./Panel";

function OverallInfraView({
  systemState,
  appState,
  closePreviewForm,
  openVdbeForm,
  closeVdbeForm,
  setPreviewBlueprint,
}) {
  const { previewForm, vdbeForm } = appState;
  const [highlight, setHighlight] = useState({
    hoveredVdbe: null,
    hoveredEngine: null,
  });
  const setVdbeHighlight = useCallback((vdbeName) => {
    setHighlight({ hoveredVdbe: vdbeName, hoveredEngine: null });
  }, []);
  const setEngineHighlight = useCallback((engine) => {
    setHighlight({ hoveredVdbe: null, hoveredEngine: engine });
  }, []);
  const clearHighlight = useCallback(() => {
    setHighlight({ hoveredVdbe: null, hoveredEngine: null });
  }, []);
  const highlightContextValue = {
    highlight,
    setVdbeHighlight,
    setEngineHighlight,
    clearHighlight,
  };

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
    <HighlightContext.Provider value={highlightContextValue}>
      <div className="infra-view column" style={{ flexGrow: 3 }}>
        <h2 className="col-h2">
          <StorageRoundedIcon style={{ marginRight: "8px" }} />
          Data Infrastructure
        </h2>
        <div className="column-inner">
          <Panel>
            {previewForm.open && (
              <WorkloadInput
                initialEngineIntensities={systemState.virtual_infra.engines.map(
                  (engine) => ({ name: engine.name, intensity: 1 }),
                )}
                min={1}
                max={10}
                onClose={closePreviewForm}
                setPreviewBlueprint={setPreviewBlueprint}
              />
            )}
            {vdbeForm.open && (
              <CreateEditVdbeForm
                currentVdbe={vdbeForm.shownVdbe}
                allTables={allTables}
                onCloseClick={closeVdbeForm}
              />
            )}
            <VirtualInfraView
              virtualInfra={systemState.virtual_infra}
              onAddVdbeClick={() => openVdbeForm(null)}
              onEditVdbeClick={openVdbeForm}
              disableVdbeChanges={previewForm.open || vdbeForm.open}
            />
            <div className="infra-separator" />
            <BlueprintView
              blueprint={systemState.blueprint}
              nextBlueprint={systemState.next_blueprint}
              previewBlueprint={previewForm.shownPreviewBlueprint}
            />
          </Panel>
        </div>
      </div>
    </HighlightContext.Provider>
  );
}

export default OverallInfraView;
