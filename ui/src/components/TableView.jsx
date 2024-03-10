import "./styles/TableView.css";

function WriterMarker({ color }) {
  return <div class={`db-table-view-writer ${color}`}>W</div>;
}

function TableView({ name, isWriter, color }) {
  return (
    <div class="db-table-view">
      {name}
      {isWriter && <WriterMarker color={color} />}
    </div>
  );
}

export default TableView;
