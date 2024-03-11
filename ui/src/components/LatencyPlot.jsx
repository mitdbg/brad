import {
  Chart as ChartJS,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
);

function LatencyPlot({
  seriesName,
  labels,
  values,
  xLabel,
  yLabel,
  shadeSeconds,
}) {
  const labelSize = 14;
  const options = {
    scales: {
      y: {
        beginAtZero: true,
        grace: "10%",
        title: {
          display: true,
          text: yLabel,
          font: {
            size: labelSize,
          },
        },
      },
      x: {
        beginAtZero: true,
        grace: "10%",
        title: {
          display: true,
          text: xLabel,
          font: {
            size: labelSize,
          },
        },
        ticks: {
          minRotation: 45,
          maxRotation: 45,
          maxTicksLimit: 10,
        },
      },
    },
    plugins: {
      legend: false,
    },
  };

  const data = {
    labels,
    datasets: [
      {
        label: seriesName,
        data: values,
        backgroundColor: "rgb(29, 128, 51)",
        borderColor: "rgba(29, 128, 51, 0.8)",
      },
    ],
  };

  if (shadeSeconds != null) {
    data.datasets.push({
      data: labels.map(() => shadeSeconds),
      fill: true,
      backgroundColor: "rgba(0, 0, 0, 0.025)",
      pointRadius: 0,
    });
  }

  return <Line data={data} options={options} />;
}

export default LatencyPlot;
