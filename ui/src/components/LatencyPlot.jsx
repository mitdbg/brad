import {
  Chart as ChartJS,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
);

function LatencyPlot({ seriesName, labels, values, xLabel, yLabel }) {
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

  return <Line data={data} options={options} />;
}

export default LatencyPlot;
