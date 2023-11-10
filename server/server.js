const express = require("express");
const app = express();
const fs = require("fs");
const cors = require("cors");
const PORT = 5000;

app.use(cors());

app.get("/api", (req, res) => {
  console.log("Recibida solicitud a /api");
  fs.readFile("sample.json", "utf8", (err, data) => {
    if (err) {
      console.error("Erroren la lectura del archivo JSON:", err);
      res.status(500).json({ error: "Error en la lectura del archivo JSON" });
      return;
    }
    const jsonData = JSON.parse(data);
    res.json(jsonData);
  });
});

app.listen(PORT, () => {
  console.log(`Servidor corriendo en el puerto ${PORT}`);
});



const [backendData, setBackendData] = useState({
    total: 0,
    entries: [],
  });


  