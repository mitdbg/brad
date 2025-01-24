import "./styles/TableView.css";

function WriterMarker({ color }) {
  return <div class={`db-table-view-writer ${color}`}>W</div>;
}

function TableView({
  name,
  isWriter,
  color,
  onTableHoverEnter,
  onTableHoverExit,
  onTableClick,
  highlightClass,
}) {
  let handleTableClick = onTableClick;
  if (handleTableClick == null) {
    handleTableClick = () => {};
  }
  return (
    <div
      class={`db-table-view ${highlightClass} ${onTableClick != null ? "clickable" : ""}`}
      onMouseEnter={onTableHoverEnter}
      onMouseLeave={onTableHoverExit}
      onClick={() => handleTableClick(name)}
    >
      {name}
      {isWriter && <WriterMarker color={color} />}
    </div>
  );
}

export default TableView;
