import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, NavLink, Route, Routes } from "react-router-dom";
import "./style.css";
import Runs from "./pages/Runs";
import RunDetail from "./pages/RunDetail";
import Approvals from "./pages/Approvals";
import Diff from "./pages/Diff";

function ApiKey() {
  const [v, setV] = React.useState(localStorage.getItem("keel.apiKey") || "");
  return (
    <input className="apikey" placeholder="API key (optional)" value={v} type="password"
      onChange={(e) => { setV(e.target.value); localStorage.setItem("keel.apiKey", e.target.value); }} />
  );
}

function App() {
  return (
    <BrowserRouter>
      <header>
        <span className="brand">keel</span>
        <nav>
          <NavLink to="/" end>Runs</NavLink>
          <NavLink to="/approvals">Approvals</NavLink>
          <NavLink to="/diff">Diff</NavLink>
        </nav>
        <ApiKey />
      </header>
      <main>
        <Routes>
          <Route path="/" element={<Runs />} />
          <Route path="/runs/:id" element={<RunDetail />} />
          <Route path="/approvals" element={<Approvals />} />
          <Route path="/diff" element={<Diff />} />
        </Routes>
      </main>
    </BrowserRouter>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
