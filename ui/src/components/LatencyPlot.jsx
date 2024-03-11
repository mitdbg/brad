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
  const options = {
    scales: {
      y: {
        beginAtZero: true,
        grace: "10%",
        title: {
          display: true,
          text: yLabel,
        },
      },
      x: {
        beginAtZero: true,
        grace: "10%",
        title: {
          display: true,
          text: xLabel,
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
        borderColor: "rgb(255, 99, 132)",
        backgroundColor: "rgba(255, 99, 132, 0.5)",
      },
    ],
  };

  return <Line data={data} options={options} />;
}

export default LatencyPlot;
