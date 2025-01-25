import VdbeView from "./VdbeView";
import AddCircleOutlineRoundedIcon from "@mui/icons-material/AddCircleOutlineRounded";
import Button from "@mui/material/Button";
import "./styles/VirtualInfraView.css";

function VirtualInfraView({
  virtualInfra,
  highlight,
  onTableHoverEnter,
  onTableHoverExit,
  onAddVdbeClick,
  onEditVdbeClick,
  disableVdbeChanges,
}) {
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
            editable={!disableVdbeChanges}
            onEditClick={onEditVdbeClick}
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
          onClick={onAddVdbeClick}
          disabled={disableVdbeChanges}
        >
          Add New VDBE
        </Button>
      </div>
    </div>
  );
}

export default VirtualInfraView;
