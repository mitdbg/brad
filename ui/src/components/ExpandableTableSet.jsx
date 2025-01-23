import { useState } from "react";
import ArrowDropDownRoundedIcon from "@mui/icons-material/ArrowDropDownRounded";
import ArrowDropUpRoundedIcon from "@mui/icons-material/ArrowDropUpRounded";
import "./styles/ExpandableTableSet.css";

function ShowMore({ onClick, total }) {
  return (
    <div className="expandable-table-set-button show-more" onClick={onClick}>
      <ArrowDropDownRoundedIcon />
      Show More ({total - 6})
      <ArrowDropDownRoundedIcon />
    </div>
  );
}

function ShowLess({ onClick }) {
  return (
    <div className="expandable-table-set-button show-less" onClick={onClick}>
      <ArrowDropUpRoundedIcon />
      Show Less
      <ArrowDropUpRoundedIcon />
    </div>
  );
}

function ExpandableTableSet({ children }) {
  const needsExpansion = children.length > 6;
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="expandable-table-set-wrap">
      <div
        className={`expandable-table-set ${needsExpansion && !expanded ? "expandable" : ""}`}
      >
        <div className="expandable-table-set-inner">{children}</div>
      </div>
      {needsExpansion && !expanded && (
        <ShowMore onClick={() => setExpanded(true)} total={children.length} />
      )}
      {needsExpansion && expanded && (
        <ShowLess onClick={() => setExpanded(false)} />
      )}
    </div>
  );
}

export default ExpandableTableSet;
