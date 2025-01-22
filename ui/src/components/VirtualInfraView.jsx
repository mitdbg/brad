import Panel from "./Panel";
import VdbeView from "./VdbeView";
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
    <Panel heading="Virtual Database Engines" className="infra-column-panel">
      <div class="vdbe-view-wrap">
        {virtualInfra?.engines?.map((vdbe, index) => (
          <VdbeView
            key={vdbe.name}
            highlight={highlight}
            onTableHoverEnter={onTableHoverEnter}
            onTableHoverExit={onTableHoverExit}
            workloadState={workloadStates[index]}
            updateWorkloadNumClients={(numClients) =>
              updateWorkloadNumClients(index, numClients)
            }
            vdbe={vdbe}
          />
        ))}
      </div>
    </Panel>
  );
}

export default VirtualInfraView;
