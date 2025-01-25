import "./styles/Header.css";
import BuildRoundedIcon from "@mui/icons-material/BuildRounded";
import bradLogo from "../assets/brad_logo.png";

function statusToDisplay(status) {
  if (status === "transitioning") {
    return "Transitioning...";
  } else if (status === "planning") {
    return "Running planner...";
  } else {
    return "Running";
  }
}

function StatusText({ status, schema }) {
  if (!!schema) {
    return (
      <div class="header-status-text">
        {statusToDisplay(status)} ({schema})
      </div>
    );
  } else {
    return <div class="header-status-text">{statusToDisplay(status)}</div>;
  }
}

function StatusIndicator({ status, schema }) {
  return (
    <div class="header-status">
      <div class={`header-status-icon ${status}`}></div>
      <StatusText status={status} schema={schema} />
    </div>
  );
}

function HeaderButton({ icon, children, onClick, disabled }) {
  return (
    <div
      className={`header-button ${disabled ? "disabled" : ""}`}
      onClick={onClick}
    >
      {icon}
      <span>{children}</span>
    </div>
  );
}

function Header({ status, onWorkloadClick, workloadDisabled }) {
  return (
    <div class="header">
      <div class="header-inner">
        <div className="header-left">
          <div class="header-logo">
            <div class="header-logo-img">
              <img src={bradLogo} />
            </div>
            <div class="header-logo-txt">
              <strong>BRAD</strong> Dashboard
            </div>
          </div>
          <HeaderButton
            icon={<BuildRoundedIcon />}
            onClick={onWorkloadClick}
            disabled={workloadDisabled}
          >
            Adjust Workload
          </HeaderButton>
        </div>
        <div className="header-right">
          <StatusIndicator status={status} schema="imdb_extended_100g" />
        </div>
      </div>
    </div>
  );
}

export default Header;
