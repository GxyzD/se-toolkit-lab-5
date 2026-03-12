import React, { useState, useEffect } from 'react';
import './App.css';
import Items from './Items';
import Dashboard from './Dashboard';

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [apiKey, setApiKey] = useState('');
  const [currentPage, setCurrentPage] = useState<'items' | 'dashboard'>('items');

  // Проверяем, есть ли ключ в localStorage при загрузке
  useEffect(() => {
    const savedKey = localStorage.getItem('api_key');
    if (savedKey) {
      setApiKey(savedKey);
      setIsAuthenticated(true);
    }
  }, []);

  const handleAuth = (e: React.FormEvent) => {
    e.preventDefault();
    if (apiKey.trim()) {
      localStorage.setItem('api_key', apiKey);
      setIsAuthenticated(true);
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('api_key');
    setApiKey('');
    setIsAuthenticated(false);
  };

  if (!isAuthenticated) {
    return (
      <div className="auth-container">
        <h1>Авторизация</h1>
        <form onSubmit={handleAuth}>
          <input
            type="text"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="Введите ваш API ключ"
            className="auth-input"
          />
          <button type="submit" className="auth-button">
            Войти
          </button>
        </form>
      </div>
    );
  }

  return (
    <div className="App">
      <header className="app-header">
        <h1>Autochecker Analytics</h1>
        <div className="header-controls">
          <nav className="main-nav">
            <button
              className={`nav-button ${currentPage === 'items' ? 'active' : ''}`}
              onClick={() => setCurrentPage('items')}
            >
              Элементы
            </button>
            <button
              className={`nav-button ${currentPage === 'dashboard' ? 'active' : ''}`}
              onClick={() => setCurrentPage('dashboard')}
            >
              Дашборд
            </button>
          </nav>
          <button onClick={handleLogout} className="logout-button">
            Выйти
          </button>
        </div>
      </header>
      <main className="app-main">
        {currentPage === 'items' ? <Items /> : <Dashboard />}
      </main>
    </div>
  );
}

export default App;