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
import { initializeApp, getApps } from "firebase/app";
import {
  getDatabase,
  ref,
  query,
  orderByKey,
  startAfter,
  limitToFirst,
  get,
  onChildAdded
} from "firebase/database";

// --- Configuración de Firebase ---
const firebaseConfig = {
  apiKey: process.env.FIREBASE_API_KEY || "AIzaSyAYA7zjtvLo6jcCoXgi8-PULcvXdudG_NM",
  authDomain: process.env.FIREBASE_AUTH_DOMAIN || "acueductof-a2d8f.firebaseapp.com",
  databaseURL: process.env.FIREBASE_DATABASE_URL || "https://acueductof-a2d8f-default-rtdb.firebaseio.com",
  projectId: process.env.FIREBASE_PROJECT_ID || "acueductof-a2d8f",
  storageBucket: "acueductof-a2d8f.appspot.com",
  messagingSenderId: "428972606386",
  appId: "1:428972606386:web:6568718194bd76b4885aab",
  measurementId: "G-SBS6NWMWMT"
};

// Inicializar Firebase solo si no está ya inicializado
let firebaseApp;
if (getApps().length === 0) {
  firebaseApp = initializeApp(firebaseConfig);
} else {
  firebaseApp = getApps()[0];
}

const db = getDatabase(firebaseApp);

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
function loadRecentIDs(linesToRead = 1000) {
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
  let totalDownloaded = 0;
  const MAX_RECORDS = 1000; // Máximo 1000 registros totales
  console.log("⏳ Descargando datos iniciales...");

  while (!finished && totalDownloaded < MAX_RECORDS) {
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
        totalDownloaded++;
        if (totalDownloaded >= MAX_RECORDS) break;
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
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

app.get('/api/latest-data', (req, res) => {
  const { limit = 100, offset = 0 } = req.query;
  try {
    const stats = fs.statSync(CACHE_FILE);
    const size = stats.size;
    const chunkSize = 100000; // 100KB por chunk
    const chunks = Math.ceil(size / chunkSize);
    const requestedChunk = Math.floor(offset / 1000);
    
    if (requestedChunk >= chunks) {
      return res.json([]);
    }
    
    const fd = fs.openSync(CACHE_FILE, 'r');
    const buffer = Buffer.alloc(chunkSize);
    const position = requestedChunk * chunkSize;
    const bytesRead = fs.readSync(fd, buffer, 0, chunkSize, position);
    fs.closeSync(fd);
    
    const lines = buffer.slice(0, bytesRead).toString().split('\n').filter(line => line.trim());
    const records = lines.slice(offset % 1000, (offset % 1000) + parseInt(limit)).map(line => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    }).filter(Boolean);
    
    res.json(records);
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
  // Solo cargar IDs recientes, sin descarga masiva inicial
  loadRecentIDs(100);
  const PATH = "payload";
  // await downloadInBatches(PATH, 200); // Comentado para evitar sobrecarga
  listenForNew(PATH);
  
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`🚀 Servidor corriendo en puerto ${PORT}`);
  });
  
  // Limpiar memoria cada 30 minutos
  setInterval(() => {
    if (global.gc) {
      global.gc();
      console.log('🧹 Memoria limpiada');
    }
  }, 30 * 60 * 1000);
})();
