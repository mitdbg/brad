import "./styles/Panel.css";

function Panel({ heading, children, className }) {
  return (
    <div class={`panel ${className != null ? className : ""}`}>
      {heading && <h2>{heading}</h2>}
      {children}
    </div>
  );
}

export default Panel;
