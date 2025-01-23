import VdbeView from "./VdbeView";
import AddCircleOutlineRoundedIcon from "@mui/icons-material/AddCircleOutlineRounded";
import Button from "@mui/material/Button";
import "./styles/VirtualInfraView.css";
import { useEffect, useState, useCallback } from "react";
import { fetchWorkloadClients, setWorkloadClients } from "../api";

function VirtualInfraView({
  virtualInfra,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
  endpoints,
}) {
  const [workloadStates, setWorkloadStates] = useState([]);
  const updateWorkloadNumClients = useCallback(
    async (vdbeIndex, numClients) => {
      const { workloadRunners } = endpoints;
      if (
        vdbeIndex >= workloadRunners.length ||
        vdbeIndex >= workloadStates.length
      ) {
        return;
      }
      const endpoint = workloadRunners[vdbeIndex];
      const newWorkloadState = await setWorkloadClients(
        endpoint.port,
        numClients,
      );

      // Skip the state update if there was no change.
      const existingWorkloadState = workloadStates[vdbeIndex];
      if (
        newWorkloadState.curr_clients === existingWorkloadState.curr_clients &&
        newWorkloadState.max_clients === existingWorkloadState.max_clients
      ) {
        return;
      }

      setWorkloadStates(
        workloadStates.map((ws, index) =>
          index === vdbeIndex ? newWorkloadState : ws,
        ),
      );
    },
    [endpoints, workloadStates],
  );

  useEffect(() => {
    async function fetchRunnerState() {
      const { workloadRunners } = endpoints;
      const promises = workloadRunners.map((endpoint) =>
        fetchWorkloadClients(endpoint.port),
      );
      const results = await Promise.all(promises);
      setWorkloadStates(results);
    }
    fetchRunnerState();
  }, [endpoints]);

  return (
    <div class="infra-region vdbe-view-wrap">
      <h2>Virtual</h2>
      <div class="vdbe-view-engines-wrap">
        {virtualInfra?.engines?.map((vdbe) => (
          <VdbeView
            key={vdbe.name}
            highlight={highlight}
            onTableHoverEnter={onTableHoverEnter}
            onTableHoverExit={onTableHoverExit}
            vdbe={vdbe}
          />
        ))}
      </div>
      <div className="infra-controls">
        <Button
          startIcon={<AddCircleOutlineRoundedIcon />}
          sx={{
            color: "text.secondary",
            bgcolor: "background.paper",
            "&:hover": { bgcolor: "#f5f5f5", opacity: 1 },
            opacity: 0.8,
          }}
        >
          Add New VDBE
        </Button>
      </div>
    </div>
  );
}

export default VirtualInfraView;
