import "./styles/Panel.css";

function Panel({ heading, children }) {
  return (
    <div class="panel">
      {heading && <h2>{heading}</h2>}
      {children}
    </div>
  );
}

export default Panel;
