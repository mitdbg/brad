import { useState } from "react";
import VirtualInfraView from "./VirtualInfraView";
import BlueprintView from "./BlueprintView";
import WorkloadInput from "./WorkloadInput";
import CreateEditVdbeForm from "./CreateEditVdbeForm";
import StorageRoundedIcon from "@mui/icons-material/StorageRounded";
import Panel from "./Panel";

function OverallInfraView({
  appState,
  closeWorkloadInput,
  openVdbeForm,
  closeVdbeForm,
}) {
  const { systemState, workloadInputOpen, vdbeForm } = appState;
  const [highlight, setHighlight] = useState({
    hoverEngine: null,
    virtualEngines: {},
    physicalEngines: {},
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
    <div className="infra-view column" style={{ flexGrow: 3 }}>
      <h2 className="col-h2">
        <StorageRoundedIcon style={{ marginRight: "8px" }} />
        Data Infrastructure
      </h2>
      <div className="column-inner">
        <Panel>
          {workloadInputOpen && (
            <WorkloadInput
              engineIntensity={[]}
              min={1}
              max={10}
              onClose={closeWorkloadInput}
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
            highlight={highlight}
            onTableHoverEnter={onTableHoverEnter}
            onTableHoverExit={onTableHoverExit}
            onAddVdbeClick={() => openVdbeForm(null)}
            onEditVdbeClick={openVdbeForm}
          />
          <div className="infra-separator" />
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
  );
}

export default OverallInfraView;
