import "./styles/DbCylinder.css";

function DbCylinder({ color, children }) {
  return (
    <div class={`db-cylinder ${color || ""}`}>
      <div class="db-cylinder-inner">{children}</div>
    </div>
  );
}

export default DbCylinder;
