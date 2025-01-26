import { createContext } from "react";

const HighlightContext = createContext({
  highlight: {
    hoveredVdbe: null,
    hoveredEngine: null,
  },
  setVdbeHighlight: (vdbeName) => {},
  setEngineHighlight: (engine) => {},
  clearHighlight: () => {},
});

export default HighlightContext;
