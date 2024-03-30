import "./styles/DbCylinder.css";

function DbCylinder({ color, children, onClick }) {
  return (
    <div
      class={`db-cylinder ${color || ""} ${onClick ? "clickable" : ""}`}
      onClick={onClick}
    >
      <div class="db-cylinder-inner">{children}</div>
    </div>
  );
}

export default DbCylinder;
