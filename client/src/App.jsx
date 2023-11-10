import { BrowserRouter, Routes, Route } from "react-router-dom";
import InicioPage from "./pages/InicioPage";
import SeriesPage from "./pages/SeriesPage"
import PeliculasPage from "./pages/PeliculasPage"



function App() {
  return (
    <div>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<InicioPage />} />
          <Route path='/series' element={<SeriesPage/>}></Route>
          <Route path='/peliculas' element={<PeliculasPage/>}></Route>
        </Routes>
      </BrowserRouter>
    </div>
  );
}

export default App;
