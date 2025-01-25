import { useContext } from "react";
import HighlightContext from "./HighlightContext";
import VdbeView from "./VdbeView";
import { highlightVdbeClass } from "../highlight";

function HighlightableVdbe({ vdbe, ...props }) {
  const { highlight, setVdbeHighlight, clearHighlight } =
    useContext(HighlightContext);
  return (
    <div
      className={highlightVdbeClass(highlight, vdbe.name, vdbe.mapped_to)}
      onMouseEnter={() => setVdbeHighlight(vdbe.name)}
      onMouseLeave={clearHighlight}
    >
      <VdbeView vdbe={vdbe} {...props} />
    </div>
  );
}

export default HighlightableVdbe;
