import Header from "./components/Header";
import VirtualInfraView from "./components/VirtualInfraView";
import BlueprintView from "./components/BlueprintView";
import PerfView from "./components/PerfView";

import "./App.css";

function App() {
  return (
    <>
      <Header />
      <div class="body-container">
        <div class="column">
          <h2 class="col-h2">Data Infrastructure</h2>
          <div class="column-inner">
            <VirtualInfraView />
            <BlueprintView />
          </div>
        </div>
        <div class="column">
          <h2 class="col-h2">Performance Monitoring</h2>
          <div class="column-inner">
            <PerfView />
          </div>
        </div>
      </div>
    </>
  );
}

export default App;
