function highlightTableViewClass(
  highlightState,
  engineName,
  tableName,
  isVirtual,
) {
  const somethingHovered =
    Object.keys(highlightState.virtualEngines).length > 0 ||
    Object.keys(highlightState.physicalEngines).length > 0;
  if (!somethingHovered) {
    return "";
  }
  const relevantState = isVirtual
    ? highlightState.virtualEngines
    : highlightState.physicalEngines;
  const shouldHighlight = relevantState[engineName] === tableName;
  if (shouldHighlight) {
    return "highlight";
  } else {
    return "dim";
  }
}

export { highlightTableViewClass };
