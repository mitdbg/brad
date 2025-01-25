import { useContext } from "react";
import HighlightContext from "./HighlightContext";
import PhysDbView from "./PhysDbView";
import { highlightEngineClass } from "../highlight";

function HighlightablePhysDb({ engine, mappedVdbes, ...props }) {
  const { highlight, setEngineHighlight, clearHighlight } =
    useContext(HighlightContext);
  return (
    <div
      className={highlightEngineClass(highlight, engine, mappedVdbes)}
      onMouseEnter={() => setEngineHighlight(engine)}
      onMouseLeave={clearHighlight}
    >
      <PhysDbView engine={engine} {...props} />
    </div>
  );
}

export default HighlightablePhysDb;
