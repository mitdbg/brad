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

export { highlightTableViewClass };
