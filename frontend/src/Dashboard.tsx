import React, { useState, useEffect } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

// Регистрация компонентов Chart.js
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
);

// Типы для API ответов
interface ScoreBucket {
  bucket: string;
  count: number;
}

interface PassRate {
  task: string;
  avg_score: number;
  attempts: number;
}

interface TimelinePoint {
  date: string;
  submissions: number;
}

const Dashboard: React.FC = () => {
  const [selectedLab, setSelectedLab] = useState<string>('lab-04');
  const [scoreData, setScoreData] = useState<ScoreBucket[]>([]);
  const [passRates, setPassRates] = useState<PassRate[]>([]);
  const [timeline, setTimeline] = useState<TimelinePoint[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Получаем API ключ из localStorage
  const getAuthHeaders = () => {
    const token = localStorage.getItem('api_key');
    return {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  };

  // Загрузка всех данных
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      
      try {
        // Параллельные запросы к API
        const [scoresRes, passRatesRes, timelineRes] = await Promise.all([
          fetch(`/api/analytics/scores?lab=${selectedLab}`, { headers: getAuthHeaders() }),
          fetch(`/api/analytics/pass-rates?lab=${selectedLab}`, { headers: getAuthHeaders() }),
          fetch(`/api/analytics/timeline?lab=${selectedLab}`, { headers: getAuthHeaders() }),
        ]);

        if (!scoresRes.ok || !passRatesRes.ok || !timelineRes.ok) {
          throw new Error('Failed to fetch analytics data');
        }

        const [scores, passRatesData, timelineData] = await Promise.all([
          scoresRes.json(),
          passRatesRes.json(),
          timelineRes.json(),
        ]);

        setScoreData(scores);
        setPassRates(passRatesData);
        setTimeline(timelineData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedLab]);

  // Подготовка данных для bar chart (распределение баллов)
  const barChartData = {
    labels: scoreData.map(item => item.bucket),
    datasets: [
      {
        label: 'Количество студентов',
        data: scoreData.map(item => item.count),
        backgroundColor: 'rgba(54, 162, 235, 0.5)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  };

  // Подготовка данных для line chart (таймлайн)
  const lineChartData = {
    labels: timeline.map(item => new Date(item.date).toLocaleDateString()),
    datasets: [
      {
        label: 'Сабмиты',
        data: timeline.map(item => item.submissions),
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.5)',
        tension: 0.1,
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
    },
  };

  if (loading) {
    return <div className="dashboard-loading">Загрузка данных...</div>;
  }

  if (error) {
    return <div className="dashboard-error">Ошибка: {error}</div>;
  }

  return (
    <div className="dashboard">
      <h1>Аналитика</h1>
      
      {/* Выбор лабораторной */}
      <div className="lab-selector">
        <label htmlFor="lab-select">Выберите лабораторную: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          <option value="lab-04">Lab 04</option>
          <option value="lab-03">Lab 03</option>
          <option value="lab-02">Lab 02</option>
          <option value="lab-01">Lab 01</option>
        </select>
      </div>

      {/* Сетка графиков */}
      <div className="charts-grid">
        {/* Bar chart - распределение баллов */}
        <div className="chart-card">
          <h2>Распределение баллов</h2>
          {scoreData.length > 0 ? (
            <Bar data={barChartData} options={chartOptions} />
          ) : (
            <p>Нет данных для отображения</p>
          )}
        </div>

        {/* Line chart - таймлайн */}
        <div className="chart-card">
          <h2>Динамика сабмитов</h2>
          {timeline.length > 0 ? (
            <Line data={lineChartData} options={chartOptions} />
          ) : (
            <p>Нет данных для отображения</p>
          )}
        </div>
      </div>

      {/* Таблица с проходимостью */}
      <div className="table-card">
        <h2>Проходимость по задачам</h2>
        {passRates.length > 0 ? (
          <table className="pass-rates-table">
            <thead>
              <tr>
                <th>Задача</th>
                <th>Средний балл</th>
                <th>Количество попыток</th>
              </tr>
            </thead>
            <tbody>
              {passRates.map((item, index) => (
                <tr key={index}>
                  <td>{item.task}</td>
                  <td>{item.avg_score.toFixed(1)}%</td>
                  <td>{item.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>Нет данных для отображения</p>
        )}
      </div>
    </div>
  );
};

export default Dashboard;