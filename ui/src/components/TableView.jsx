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
  highlightClass,
}) {
  return (
    <div
      class={`db-table-view ${highlightClass}`}
      onMouseEnter={onTableHoverEnter}
      onMouseLeave={onTableHoverExit}
    >
      {name}
      {isWriter && <WriterMarker color={color} />}
    </div>
  );
}

export default TableView;
