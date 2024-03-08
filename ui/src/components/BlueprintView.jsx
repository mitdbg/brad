import Panel from "./Panel";
import PhysDbView from "./PhysDbView";
import "./styles/BlueprintView.css";

function BlueprintView() {
  return (
    <Panel heading="Current Blueprint (Physical Infrastructure)">
      <div class="bp-view-wrap">
        <PhysDbView name="Aurora" provisioning="db.r6g.xlarge(2)" />
        <PhysDbView name="Redshift" provisioning="dc2.large(2)" />
        <PhysDbView name="Athena" />
      </div>
    </Panel>
  );
}

export default BlueprintView;
