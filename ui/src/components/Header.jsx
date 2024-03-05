import "./styles/Header.css";
import dbLogo from "../assets/db.svg";

function Header() {
  return (
    <div class="header">
      <div class="header-inner">
        <div class="header-logo">
          <div class="header-logo-img"><img src={dbLogo} style={{width: "60px", height: "60px"}} /></div>
          <div class="header-logo-txt"><strong>BRAD</strong> Dashboard</div>
        </div>
      </div>
    </div>
  );
}

export default Header;
