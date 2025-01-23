import "./styles/InsetPanel.css";

function InsetPanel({ children, className }) {
  return <div className={`inset-panel-wrap ${className}`}>{children}</div>;
}

export default InsetPanel;
