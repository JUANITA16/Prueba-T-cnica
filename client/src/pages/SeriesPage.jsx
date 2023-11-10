import  { useEffect, useState } from "react";
import HeaderComponent from "../components/HeaderComponent"
import FooterComponent from "../components/FooterComponent"
import '../App.css';


const SeriesPage = () => {
  const [backendData, setBackendData] = useState({
    total: 0,
    entries: [],
  });

  useEffect(() => {
    fetch("http://localhost:5000/api")
      .then((response) => response.json())
      .then((data) => {
        console.log("Resiviendo Data:", data);
        const primeras20 = data.entries.slice(1, 21);
        setBackendData({ total: data.total, entries: primeras20 });
      })
      .catch((error) => {
        console.error("Error en la data:", error);
      });
  }, []);

  return (
    <>
    <HeaderComponent/>
<div className="container mt-3">
        <div className="row">
          {backendData.entries.map((entry, i) => (
            <div key={i} className="col-lg-3 col-md-4 col-sm-6 mb-4">
              <div className="card">
                <img
                  src={entry.images["Poster Art"].url}
                  alt={entry.title}
                  className="card-img-top"
                  style={{ maxHeight: "300px" }}
                />
                <div className="card-body">
                  <h5 className="card-title">{entry.title}</h5>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
      <FooterComponent/>
    </>
  );
};

export default SeriesPage;