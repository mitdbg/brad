function highlightVdbeClass(highlightState, vdbeName, mappedToEngine) {
  const { hoveredVdbe, hoveredEngine } = highlightState;
  if (hoveredVdbe == null && hoveredEngine == null) {
    return "";
  }

  if (hoveredVdbe != null) {
    return hoveredVdbe === vdbeName ? "highlight" : "dim";
  } else {
    return hoveredEngine === mappedToEngine ? "highlight" : "dim";
  }
}

function highlightEngineClass(highlightState, engineName, vdbeNames) {
  const { hoveredVdbe, hoveredEngine } = highlightState;
  if (hoveredVdbe == null && hoveredEngine == null) {
    return "";
  }

  if (hoveredEngine != null) {
    return hoveredEngine === engineName ? "highlight" : "dim";
  } else {
    return vdbeNames.includes(hoveredVdbe) ? "highlight" : "dim";
  }
}

// The functions below are legacy implementations of the highlight logic.

function highlightTableViewClass(
  highlightState,
  engineName,
  tableName,
  isVirtual,
) {
  if (highlightState.hoverEngine == null) {
    return "";
  }
  const relevantState = isVirtual
    ? highlightState.virtualEngines
    : highlightState.physicalEngines;
  const shouldHighlight = relevantState[engineName] === tableName;
  if (shouldHighlight) {
    return "highlight";
  } else {
    if (highlightState.hoverEngine === engineName) {
      return "dim";
    } else {
      return "hidden";
    }
  }
}

function highlightEngineViewClass(highlightState, engineName, isVirtual) {
  if (highlightState.hoverEngine == null) {
    return "";
  }
  const relevantState = isVirtual
    ? highlightState.virtualEngines
    : highlightState.physicalEngines;
  const shouldHighlight =
    relevantState[engineName] != null ||
    highlightState.hoverEngine === engineName;
  if (shouldHighlight) {
    return "highlight";
  } else {
    return "dim";
  }
}

function sortTablesToHoist(highlightState, currentEngine, isVirtual, tables) {
  const tableCopy = tables.slice();
  if (
    highlightState.hoverEngine == null ||
    highlightState.hoverEngine === currentEngine
  ) {
    return tableCopy;
  }

  let relTables = null;
  if (isVirtual) {
    relTables = highlightState.virtualEngines;
  } else {
    relTables = highlightState.physicalEngines;
  }
  if (relTables[currentEngine] == null) {
    return tableCopy;
  }

  let hoistIndex = null;
  tableCopy.forEach(({ name }, index) => {
    if (name === relTables[currentEngine]) {
      hoistIndex = index;
    }
  });
  if (hoistIndex != null && tableCopy.length > 0) {
    const tmp = tableCopy[0];
    tableCopy[0] = tableCopy[hoistIndex];
    tableCopy[hoistIndex] = tmp;
  }
  return tableCopy;
}

export {
  highlightVdbeClass,
  highlightEngineClass,

  // The functions below are legacy implementations of the highlight logic.
  highlightTableViewClass,
  highlightEngineViewClass,
  sortTablesToHoist,
};
