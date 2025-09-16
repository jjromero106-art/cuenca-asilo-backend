import fs from "fs";
import express from "express";
import cors from "cors";

const app = express();
app.use(cors());
app.use(express.static('.'));

// Sirve para leer, escribir, modificar y manejar 
                    // archivos o directorios directamente desde tu computadora
                    //  o servidor. Es como la “puerta” que le permite a tu programa 
                    // Node.js interactuar con el disco duro.
import { initializeApp } from "firebase/app";
import {
  getDatabase, // se usa para obtener una referencia a tu base de datos en tiempo real
  ref, // se usa para crear una referencia a una ubicación específica dentro de la base de datos.
  query, // filtrar, ordenar o limitar los resultados.
  orderByKey, // Esto devuelve los datos ordenados por clave
  startAfter, //Sirve para empezar a traer los resultados después de una clave o valor específico. 
  limitToFirst,// Sirve para limitar la cantidad de resultados que devuelve la consulta, 
                // empezando desde el primero (según el orden que hayas definido 
                // con orderByKey(), orderByChild(), etc.).
  get, // En Firebase, get() se usa para ejecutar una consulta una sola vez y 
        // obtener los datos en ese momento.
  onChildAdded // Se activa cada vez que se agrega un nuevo hijo (nodo) en una referencia o consulta
} from "firebase/database";

// --- Configuración de Firebase ---
const firebaseConfig = {
  apiKey: "AIzaSyAYA7zjtvLo6jcCoXgi8-PULcvXdudG_NM",
  authDomain: "acueductof-a2d8f.firebaseapp.com",
  databaseURL: "https://acueductof-a2d8f-default-rtdb.firebaseio.com",
  projectId: "acueductof-a2d8f",
  storageBucket: "acueductof-a2d8f.appspot.com",
  messagingSenderId: "428972606386",
  appId: "1:428972606386:web:6568718194bd76b4885aab",
  measurementId: "G-SBS6NWMWMT"
};

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

// --- Archivos ---
const CACHE_FILE = "firebase-cache.jsonl";
const LAST_KEY_FILE = "last-key.txt";

// --- Set de IDs procesados recientes ---
const processedIDs = new Set();

// --- Guardar y cargar última key ---
function saveLastKey(key) {
  fs.writeFileSync(LAST_KEY_FILE, key, "utf8");
}

function loadLastKey() {
  try {
    return fs.readFileSync(LAST_KEY_FILE, "utf8");
  } catch {
    return null;
  }
}

// --- Leer solo los últimos N registros del cache para evitar duplicados ---
function loadRecentIDs(linesToRead = 10000) {
  try {
    if (!fs.existsSync(CACHE_FILE)) return;
    const stats = fs.statSync(CACHE_FILE); //Obtiene la información del archivo (tamaño, fecha de creación, etc.).
    const size = stats.size; //te da el tamaño del archivo en bytes.
    const fd = fs.openSync(CACHE_FILE, "r"); //“puntero” al archivo.
    const bufferSize = Math.min(1024 * 1024, size); // máximo 1MB Define cuánto leer del archivo.
    const buffer = Buffer.alloc(bufferSize); //Reserva un buffer en memoria donde se guardará la parte del archivo que vas a leer.
    const position = Math.max(0, size - bufferSize); // Calcula desde qué punto del archivo empezar a leer.
    fs.readSync(fd, buffer, 0, bufferSize, position); // Lee el archivo:
    fs.closeSync(fd); // Cierra el archivo (buena práctica para liberar recursos).

    const lines = buffer.toString().split("\n").slice(-linesToRead); //Con .toString() lo conviertes a texto legible (string).
    lines.forEach(line => {
      if (!line.trim()) return;
      const obj = JSON.parse(line);
      processedIDs.add(obj.id);
    });

    console.log(`📂 Cargados ${processedIDs.size} IDs recientes del cache`);
  } catch (err) {
    console.warn("⚠️ No se pudo cargar cache reciente:", err);
  }
}

// --- Guardar registro en JSONL si no está duplicado ---
function appendRecord(record) {
  if (processedIDs.has(record.id)) return;
  fs.appendFileSync(CACHE_FILE, JSON.stringify(record) + "\n", "utf8");
  processedIDs.add(record.id);
}

// --- Descarga inicial por bloques desde lastKey ---
async function downloadInBatches(path, batchSize = 1000) {
  let lastKey = loadLastKey();
  let finished = false;
  console.log("⏳ Descargando datos iniciales...");

  while (!finished) {
    const q = lastKey
      ? query(ref(db, path), orderByKey(), startAfter(lastKey), limitToFirst(batchSize)) // para continuar la consulta después de la última clave que leíste.
      : query(ref(db, path), orderByKey(), limitToFirst(batchSize)); // Simplemente empieza desde el principio.

    const snap = await get(q);
    if (!snap.exists()) break;

    const data = snap.val();
    const keys = Object.keys(data);

    if (keys.length === 0) {
      finished = true;
      break;
    }

    for (const key of keys) {
      const item = data[key];
      if (item && item.sensor1 !== undefined && item.fechaa) {
        appendRecord({ id: key, sensor1: item.sensor1, fechaa: item.fechaa });
        lastKey = key;
      }
    }

    console.log(`✅ Guardado bloque con ${keys.length} registros`);
    if (lastKey) saveLastKey(lastKey);
    if (keys.length < batchSize) finished = true;
  }

  console.log("📥 Descarga inicial completa.");
}

// --- Escuchar nuevos nodos en tiempo real ---
function listenForNew(path) {
  console.log("👂 Escuchando nuevos nodos...");
  let lastKey = loadLastKey();

  const q = lastKey
    ? query(ref(db, path), orderByKey(), startAfter(lastKey))
    : query(ref(db, path), orderByKey());

  onChildAdded(q, snapshot => {
    const key = snapshot.key;
    const data = snapshot.val();
    if (!data || data.sensor1 === undefined || !data.fechaa) return;

    appendRecord({ id: key, sensor1: data.sensor1, fechaa: data.fechaa });
    saveLastKey(key);
    console.log("➕ Nuevo registro guardado:", key);
  });
}

// --- API Endpoints ---
app.get('/api/latest-data', (req, res) => {
  try {
    const lines = fs.readFileSync(CACHE_FILE, 'utf8').trim().split('\n');
    const latestRecords = lines.slice(-100).map(line => JSON.parse(line));
    res.json(latestRecords);
  } catch (error) {
    res.json([]);
  }
});

app.get('/api/last-record', (req, res) => {
  try {
    const lines = fs.readFileSync(CACHE_FILE, 'utf8').trim().split('\n');
    const lastRecord = JSON.parse(lines[lines.length - 1]);
    res.json(lastRecord);
  } catch (error) {
    res.json(null);
  }
});

// --- Ejecución ---
(async () => {
  loadRecentIDs(10000);
  const PATH = "payload";
  await downloadInBatches(PATH, 1000);
  listenForNew(PATH);
  
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`🚀 Servidor corriendo en puerto ${PORT}`);
  });
})();
