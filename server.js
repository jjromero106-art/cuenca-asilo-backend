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

// --- Leer TODOS los registros del cache para evitar duplicados ---
function loadRecentIDs(linesToRead = 50000) {
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
      try {
        const obj = JSON.parse(line);
        if (obj && obj.id) {
          processedIDs.add(obj.id);
        }
      } catch (error) {
        // Ignorar líneas corruptas silenciosamente
        console.warn(`Línea corrupta ignorada: ${line.substring(0, 50)}...`);
      }
    });

    console.log(`📂 Cargados ${processedIDs.size} IDs recientes del cache`);
  } catch (err) {
    console.warn("⚠️ No se pudo cargar cache reciente:", err);
  }
}

// --- Guardar registro en JSONL si no está duplicado ---
function appendRecord(record) {
  if (!record || !record.id || processedIDs.has(record.id)) return;
  
  try {
    // Validar que el record sea válido antes de guardar
    if (record.sensor1 !== undefined && record.fechaa) {
      const jsonLine = JSON.stringify(record) + "\n";
      fs.appendFileSync(CACHE_FILE, jsonLine, "utf8");
      processedIDs.add(record.id);
    }
  } catch (error) {
    console.error('Error guardando registro:', error, record);
  }
}

// --- Descarga inicial por bloques desde lastKey ---
async function downloadInBatches(path, batchSize = 1000) {
  let lastKey = loadLastKey();
  let finished = false;
  let totalDownloaded = 0;
  console.log("⏳ Descargando TODOS los datos iniciales...");

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
        totalDownloaded++;
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
  const { limit = 2000, offset = 0 } = req.query; // Límite más alto por defecto
  const limitNum = parseInt(limit);
  const offsetNum = parseInt(offset);
  
  try {
    if (!fs.existsSync(CACHE_FILE)) {
      return res.json([]);
    }
    
    // Leer archivo de manera más eficiente
    const content = fs.readFileSync(CACHE_FILE, 'utf8');
    const lines = content.trim().split('\n').filter(line => line.trim());
    
    // Aplicar paginación rápida
    const startIndex = offsetNum;
    const endIndex = startIndex + limitNum;
    const requestedLines = lines.slice(startIndex, endIndex);
    
    // Parsear en lote más eficiente
    const records = [];
    for (let i = 0; i < requestedLines.length; i++) {
      try {
        records.push(JSON.parse(requestedLines[i]));
      } catch (error) {
        // Ignorar líneas corruptas silenciosamente
        continue;
      }
    }
    
    // Headers para caché y compresión
    res.setHeader('Cache-Control', 'public, max-age=60'); // Cache 1 minuto
    res.setHeader('Content-Type', 'application/json');
    res.json(records);
    
  } catch (error) {
    console.error('Error en /api/latest-data:', error);
    res.status(500).json([]);
  }
});

app.get('/api/last-record', (req, res) => {
  try {
    const lines = fs.readFileSync(CACHE_FILE, 'utf8').trim().split('\n').filter(line => line.trim());
    const lastRecord = JSON.parse(lines[lines.length - 1]);
    res.json(lastRecord);
  } catch (error) {
    res.json(null);
  }
});

app.get('/api/data-info', (req, res) => {
  try {
    const lines = fs.readFileSync(CACHE_FILE, 'utf8').trim().split('\n').filter(line => line.trim());
    const records = lines.map(line => JSON.parse(line));
    
    const dates = records.map(r => new Date(r.fechaa)).sort((a, b) => a - b);
    const info = {
      total: records.length,
      firstDate: dates[0]?.toISOString(),
      lastDate: dates[dates.length - 1]?.toISOString(),
      lastRecord: records[records.length - 1]
    };
    
    res.json(info);
  } catch (error) {
    res.json({ error: error.message });
  }
});

app.get('/api/day-data', (req, res) => {
  const { date } = req.query; // Formato: YYYY-MM-DD
  
  if (!date) {
    return res.status(400).json({ error: 'Fecha requerida' });
  }
  
  try {
    const targetDate = new Date(date);
    const startOfDay = new Date(targetDate.getFullYear(), targetDate.getMonth(), targetDate.getDate());
    const endOfDay = new Date(targetDate.getFullYear(), targetDate.getMonth(), targetDate.getDate(), 23, 59, 59, 999);
    
    const content = fs.readFileSync(CACHE_FILE, 'utf8');
    const lines = content.trim().split('\n').filter(line => line.trim());
    
    const dayData = [];
    
    // Búsqueda rápida - solo parsear líneas que contengan la fecha
    const dateStr = date; // YYYY-MM-DD
    const filteredLines = lines.filter(line => line.includes(dateStr));
    
    filteredLines.forEach(line => {
      try {
        const record = JSON.parse(line);
        const recordDate = new Date(record.fechaa);
        
        if (recordDate >= startOfDay && recordDate <= endOfDay) {
          dayData.push({
            id: record.id,
            sensor1: record.sensor1,
            fechaa: record.fechaa
          });
        }
      } catch (error) {
        // Ignorar líneas corruptas
      }
    });
    
    // Ordenar por fecha
    dayData.sort((a, b) => new Date(a.fechaa) - new Date(b.fechaa));
    
    res.setHeader('Cache-Control', 'public, max-age=300'); // Cache 5 minutos
    res.json(dayData);
    
  } catch (error) {
    console.error('Error en /api/day-data:', error.message);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

// --- Ejecución ---
(async () => {
  // Optimizado para 512MB - cargar solo lo necesario
  loadRecentIDs(2000);
  const PATH = "payload";
  
  try {
    await downloadInBatches(PATH, 300); // Lotes pequeños para 512MB
    console.log('✅ Descarga inicial completada');
  } catch (error) {
    console.error('❌ Error en descarga inicial:', error);
  }
  
  listenForNew(PATH);
  
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`🚀 Servidor corriendo en puerto ${PORT}`);
  });
  
  // Limpiar memoria cada 5 minutos para 512MB
  setInterval(() => {
    if (global.gc) {
      global.gc();
      console.log('🧹 Memoria limpiada');
    }
    // Limpiar Set si crece mucho
    if (processedIDs.size > 5000) {
      const idsArray = Array.from(processedIDs);
      processedIDs.clear();
      idsArray.slice(-2000).forEach(id => processedIDs.add(id));
      console.log('🧹 IDs optimizados');
    }
  }, 5 * 60 * 1000);
})();
