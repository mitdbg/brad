function extractMetrics({ metrics }, metricName, multiplier) {
  if (multiplier == null) {
    multiplier = 1.0;
  }
  if (!Object.prototype.hasOwnProperty.call(metrics, metricName)) {
    return {
      x: [],
      y: [],
    };
  } else {
    const innerMetrics = metrics[metricName];
    return {
      x: innerMetrics.timestamps.map((val) => val.toLocaleTimeString("en-US")),
      y: innerMetrics.values.map((val) => val * multiplier),
    };
  }
}

function parseMetrics({ named_metrics }) {
  const result = {};
  Object.entries(named_metrics).forEach(([metricName, metricValues]) => {
    const parsedTs = metricValues.timestamps.map(
      (timestamp) => new Date(timestamp),
    );
    result[metricName] = {
      timestamps: parsedTs,
      values: metricValues.values,
    };
  });
  return result;
}

export { extractMetrics, parseMetrics };
