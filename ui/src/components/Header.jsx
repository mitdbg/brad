import "./styles/Header.css";
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

function Header({ status }) {
  return (
    <div class="header">
      <div class="header-inner">
        <div class="header-logo">
          <div class="header-logo-img">
            <img src={bradLogo} />
          </div>
          <div class="header-logo-txt">
            <strong>BRAD</strong> Dashboard
          </div>
        </div>
        <StatusIndicator status={status} schema="imdb_extended_100g" />
      </div>
    </div>
  );
}

export default Header;
