import "./styles/Header.css";
import dbLogo from "../assets/db.svg";

function StatusText({ status, schema }) {
  if (!!schema) {
    return (
      <div class="header-status-text">
        {status} ({schema})
      </div>
    );
  } else {
    return <div class="header-status-text">{status}</div>;
  }
}

function StatusIndicator({ status, schema }) {
  return (
    <div class="header-status">
      <div class="header-status-icon"></div>
      <StatusText status={status} schema={schema} />
    </div>
  );
}

function Header() {
  return (
    <div class="header">
      <div class="header-inner">
        <div class="header-logo">
          <div class="header-logo-img">
            <img src={dbLogo} style={{ width: "60px", height: "60px" }} />
          </div>
          <div class="header-logo-txt">
            <strong>BRAD</strong> Dashboard
          </div>
        </div>
        <StatusIndicator status="Connected" schema="imdb_extended_100g" />
      </div>
    </div>
  );
}

export default Header;