import { useCallback, useState } from "react";
import VirtualInfraView from "./VirtualInfraView";
import BlueprintView from "./BlueprintView";
import WorkloadInput from "./WorkloadInput";
import CreateEditVdbeForm from "./CreateEditVdbeForm";
import StorageRoundedIcon from "@mui/icons-material/StorageRounded";
import Snackbar from "@mui/material/Snackbar";
import HighlightContext from "./HighlightContext";
import ConfirmDialog from "./ConfirmDialog";
import Panel from "./Panel";
import { deleteVdbe } from "../api";

function OverallInfraView({
  systemState,
  appState,
  closePreviewForm,
  openVdbeForm,
  closeVdbeForm,
  setPreviewBlueprint,
  refreshData,
}) {
  const { previewForm, vdbeForm } = appState;
  const [showVdbeChangeSuccess, setShowVdbeChangeSuccess] = useState(false);
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

  const onVdbeChangeSuccess = async () => {
    await refreshData();
    setShowVdbeChangeSuccess(true);
  };
  const handleSnackbarClose = (event, reason) => {
    if (reason === "clickaway") {
      return;
    }
    setShowVdbeChangeSuccess(false);
  };

  const [deletionState, setDeletionState] = useState({
    showConfirm: false,
    deletingVdbe: null,
  });
  const openConfirmDelete = (vdbe) => {
    setDeletionState({ showConfirm: true, deletingVdbe: vdbe });
  };
  const doDeleteVdbe = async () => {
    await deleteVdbe(deletionState.deletingVdbe.internal_id);
    await refreshData();
    setDeletionState({ showConfirm: false, deletingVdbe: null });
    setShowVdbeChangeSuccess(true);
  };

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
                blueprint={systemState.blueprint}
                allTables={systemState.all_tables}
                onCloseClick={closeVdbeForm}
                onVdbeChangeSuccess={onVdbeChangeSuccess}
              />
            )}
            <VirtualInfraView
              virtualInfra={systemState.virtual_infra}
              onAddVdbeClick={() => openVdbeForm(null)}
              onEditVdbeClick={openVdbeForm}
              onDeleteVdbeClick={openConfirmDelete}
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
        <ConfirmDialog
          open={deletionState.showConfirm}
          title="Confirm Deletion"
          onCancel={() =>
            setDeletionState({ showConfirm: false, deletingVdbe: null })
          }
          onConfirm={doDeleteVdbe}
        >
          Are you sure you want to delete the VDBE "
          {deletionState.deletingVdbe?.name}"? This action cannot be undone.
        </ConfirmDialog>
        <Snackbar
          open={showVdbeChangeSuccess}
          autoHideDuration={3000}
          message="VDBE changes successfully saved."
          onClose={handleSnackbarClose}
        />
      </div>
    </HighlightContext.Provider>
  );
}

export default OverallInfraView;
