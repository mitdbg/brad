import Panel from "./Panel";
import LatencyPlot from "./LatencyPlot";
import "./styles/PerfView.css";

function PerfView() {
  return (
    <Panel>
      <div class="perf-view-wrap">
        <h2>VDBE 1: Query Latency</h2>
        <LatencyPlot
          seriesName="VDBE 1"
          labels={[0, 1, 2, 3]}
          values={[50, 51, 52]}
        />
      </div>
    </Panel>
  );
}

export default PerfView;
