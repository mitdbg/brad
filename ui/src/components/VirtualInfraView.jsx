import axios from "axios";
import Panel from "./Panel";
import VdbeView from "./VdbeView";
import "./styles/VirtualInfraView.css";
import { useEffect, useState, useCallback } from "react";

function baseEndpointFromObj({ host, port }) {
  return `http://${host}:${port}`;
}

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
      const baseEndpoint = baseEndpointFromObj(workloadRunners[vdbeIndex]);
      const result = await axios.post(`${baseEndpoint}/clients`, {
        curr_clients: numClients,
      });
      const newWorkloadState = result.data;

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
    [endpoints],
  );

  useEffect(async () => {
    const { workloadRunners } = endpoints;
    const promises = workloadRunners
      .map(baseEndpointFromObj)
      .map((baseEndpoint) => axios.get(`${baseEndpoint}/clients`));
    try {
      const results = await Promise.all(promises);
      setWorkloadStates(results.map(({ data }) => data));
    } catch (e) {
      console.error("Loading error", e);
    }
  }, [endpoints]);

  return (
    <Panel heading="Virtual Database Engines" className="infra-column-panel">
      <div class="vdbe-view-wrap">
        {virtualInfra?.engines?.map(({ name, ...props }, index) => (
          <VdbeView
            key={name}
            name={name}
            highlight={highlight}
            onTableHoverEnter={onTableHoverEnter}
            onTableHoverExit={onTableHoverExit}
            workloadState={workloadStates[index]}
            updateWorkloadNumClients={(numClients) =>
              updateWorkloadNumClients(index, numClients)
            }
            {...props}
          />
        ))}
      </div>
    </Panel>
  );
}

export default VirtualInfraView;
