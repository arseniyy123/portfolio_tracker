import React, { useState, useEffect } from "react";
import axios from "axios";
import { Line } from "react-chartjs-2";
import { Chart, registerables } from "chart.js";
import 'chartjs-adapter-date-fns';

Chart.register(...registerables);

const LandingPage = () => {
  const [files, setFiles] = useState({});
  const [metrics, setMetrics] = useState(null);
  const [portfolioData, setPortfolioData] = useState(null);

  const handleFileChange = (event) => {
    const { name, files } = event.target;
    setFiles((prev) => ({ ...prev, [name]: files[0] }));
  };

  const handleUpload = async () => {
    const formData = new FormData();
    formData.append("account", files.account);
    formData.append("portfolio", files.portfolio);

    try {
      const response = await axios.post("http://localhost:8000/upload", formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });
      setMetrics(response.data);
    } catch (error) {
      console.error("Error uploading files:", error);
    }
  };
  console.log(metrics);
  useEffect(() => {
    if (metrics) {
      const chartData = {
        labels: metrics.historical_portfolio_value.map((data) => data.date) || [],
        datasets: [
          {
            label: "Added Funds",
            data: metrics.historical_cashflow.map((data) => data.value) || [],
            fill: false,  // Set fill to false to avoid hiding the line
            tension: 0.4,
            borderColor: "#007bff",
            backgroundColor: "rgba(0, 123, 255, 0.1)",
            pointRadius: 0,
          },
          {
            label: "Profit & Loss",
            data: metrics.historical_portfolio_value.map((data) => data.value) || [],
            fill: false,
            tension: 0.4,
            borderColor: "red", // Red line for historical portfolio value
            pointRadius: 0,
          },
          {
            label: "Total Portfolio Value",
            data: metrics.combined_data.map((data) => data.value) || [],
            fill: true,
            tension: 0.4,
            borderColor: "black",
            pointRadius: 0,
          },
        ],
      };
      setPortfolioData(chartData);
    }
  }, [metrics]);

  return (
    <div className="flex flex-col items-center justify-center h-screen bg-gray-100 text-center">
      <h1 className="text-4xl font-bold mb-4">Welcome to the Portfolio Tracker</h1>
      <p className="mb-8 text-lg">
        Keep track of your investments, including shares and cryptocurrencies, all in one place.
        Our intuitive interface allows you to monitor performance metrics, costs, and fees effortlessly.
      </p>
      <div className="space-x-4">
        <button className="px-4 py-2 font-semibold text-white bg-blue-600 rounded hover:bg-blue-500">Sign In</button>
        <button className="px-4 py-2 font-semibold text-white bg-green-600 rounded hover:bg-green-500">Sign Up</button>
      </div>

      <div>
        <h2>Upload CSV Files</h2>
        <p>Upload your transactions, account, and portfolio CSV files to get started.</p>
        <input type="file" name="transactions" onChange={handleFileChange} />
        <input type="file" name="account" onChange={handleFileChange} />
        <input type="file" name="portfolio" onChange={handleFileChange} />
        <button onClick={handleUpload}>Upload</button>

        {metrics && (
          <div>
            <h3>Metrics</h3>
            <p>Total Dividends: {metrics.total_dividends}</p>
            <p>Total Fees: {metrics.total_fees}</p>
            <h4>Fee Breakdown:</h4>
            {metrics.fee_breakdown && (
              <ul>
                {Object.entries(metrics.fee_breakdown).map(([feeType, amount]) => (
                  <li key={feeType}>
                    {feeType}: {amount}
                  </li>
                ))}
              </ul>
            )}
            <p>Profit/Loss: {metrics.profit_loss}</p>
            <p>Portfolio Value: {metrics.portfolio_value}</p>
            <p>Cash Balance: {metrics.cash_balance}</p>
            {metrics.annual_growth_rate && (
              <p>Annual Growth Rate: {metrics.annual_growth_rate}%</p>
            )}
            {portfolioData && (
              <div style={{ width: "80%", height: "400px", margin: "auto" }}>
                <Line
                  data={portfolioData}
                  options={{
                    maintainAspectRatio: false,
                    scales: {
                      x: {
                        type: 'time',
                        time: {
                          unit: 'day',
                          tooltipFormat: 'yyyy MMM dd',
                        },
                        title: {
                          display: true,
                          text: 'Date',
                        },
                      },
                      y: {
                        title: {
                          display: true,
                          text: 'Value (€)',
                        },
                        beginAtZero: true,
                      },
                    },
                    plugins: {
                      legend: {
                        display: true,
                        position: 'top',
                      },
                    },
                  }}
                />
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default LandingPage;
