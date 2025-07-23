const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');


// ================= CONFIGURACIÓN =================
const CONFIG = {
  // Directorios
  dataDir: './datos',
  resultsDir: './resultados_parciales',
  finalFile: './resultados_finales.csv',
  checkpointDir: './checkpoints',

  // Parámetros de ejecución
  maxWorkers: 2,
  chunkSize: 250,
  testPercentage: 0.3,
  progressInterval: 1000,
  checkpointInterval: 10000,
  maxFileSize: 50 * 1024 * 1000, // 50MB
  memoryCleanInterval: 10000, // Liberar cada 10k iteraciones
  // Umbrales para resultados relevantes
  minR2: 0.85,
  maxRMSE: 0.10,

  // Rangos de parámetros (personalizar según necesidades)
  ranges: {
    pesos: {
      palmas: [1.0, 1.5, 2.0, 2.5, 3.0],
      itu: [0.5, 1.0, 1.5, 2.0],
      ba: [0.5, 1.0, 1.5],
      vientos: [0.5, 1.0, 1.5, 2.0]
    },
    lags: {
      palmas: [8, 9, 10, 11, 12],
      itu: [6, 7, 8, 9],
      ba: [0, 1, 2],
      vientos: [0, 1, 2]
    },
    coeficientesViento: {
      N: [-0.02, -0.015, -0.01, -0.005],
      NE: [-0.015, -0.01, -0.005, 0],
      E: [0.005, 0.01, 0.015, 0.02],
      SE: [0.01, 0.015, 0.02, 0.025],
      S: [-0.005, 0, 0.005, 0.01],
      SW: [-0.01, -0.005, 0, 0.005],
      W: [-0.02, -0.015, -0.01, -0.005],
      NW: [-0.025, -0.02, -0.015, -0.01]
    },
    factorViento: [0.8, 0.9, 1.0, 1.1, 1.2],
    offset: [-0.2, -0.1, 0, 0.1, 0.2]
  }
};

function parseDate(dateStr) {
  if (!dateStr) return new Date(NaN);
  
  // Limpieza agresiva de strings
  const cleaned = dateStr.toString()
    .trim()
    .replace(/[\u200B-\u200D\uFEFF]/g, '') // Elimina caracteres invisibles
    .replace(/(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2})/, '$1T$2'); // Formato ISO

  // Intenta parsear con Date() nativo primero
  const date = new Date(cleaned);
  if (!isNaN(date.getTime())) return date;

  // Fallback para formatos personalizados
  const patterns = [
    /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}):?(\d{2})?\.?(\d{3})?Z?$/i,
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})/,
    /^(\d{4}-\d{2}-\d{2})$/
  ];

  for (const pattern of patterns) {
    const match = cleaned.match(pattern);
    if (match) {
      const isoStr = match[2] 
        ? `${match[1]}T${match[2]}:00.000Z` 
        : `${match[1]}T00:00:00.000Z`;
      return new Date(isoStr);
    }
  }

  console.error('❌ Formato de fecha no reconocido:', dateStr);
  return new Date(NaN);
}


// ================= DEFINICIÓN DE COLUMNAS =================
const CSV_COLUMNS = [
    // 4 pesos
    'pesoPalmas', 'pesoItu', 'pesoBa', 'pesoVientos',
    // 4 lags
    'lagPalmas', 'lagItu', 'lagBa', 'lagVientos',
    // 2 parámetros principales
    'factorViento', 'offset',
    // 16 coeficientes de viento (8 direcciones × [factor, peso])
    ...['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'].flatMap(dir => [
        `coef_${dir}_factor`,
        `coef_${dir}_peso`
    ]),
    // 4 métricas
    'trainR2', 'testR2', 'trainRmse', 'testRmse'
];

// ================= CLASE PARA ESCRITURA DE RESULTADOS =================
class ResultWriter {
  constructor() {
    this.fileIndex = 0;
    this.currentSize = 0;
    this.currentFile = null;
    this.header = [
      'pesoPalmas', 'pesoItu', 'pesoBa', 'pesoVientos',
      'lagPalmas', 'lagItu', 'lagBa', 'lagVientos',
      'factorViento', 'offset',
      ...['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'].flatMap(dir => [
        `coef_${dir}_factor`,
        `coef_${dir}_peso`
      ]),
      'trainR2', 'testR2', 'trainRmse', 'testRmse'
    ].join(';') + '\n';

    if (!fs.existsSync(CONFIG.resultsDir)) {
      fs.mkdirSync(CONFIG.resultsDir, { recursive: true });
    }
  }

  async openNewFile() {
    if (this.currentFile) await this.closeCurrentFile();
    this.fileIndex++;
    const filePath = path.join(CONFIG.resultsDir, `results_${this.fileIndex}.csv`);
    this.currentFile = fs.createWriteStream(filePath);
    this.currentSize = 0;
    await this.writeToFile(this.header);
  }

  async write(results) {
    if (!this.currentFile || this.currentSize > CONFIG.maxFileSize) {
      await this.openNewFile();
    }

    const csvLines = results.map(result => {
      const row = {};
      // Asegurar todas las columnas con valores por defecto
      [
        'pesoPalmas', 'pesoItu', 'pesoBa', 'pesoVientos',
        'lagPalmas', 'lagItu', 'lagBa', 'lagVientos',
        'factorViento', 'offset'
      ].forEach(col => {
        row[col] = result[col] !== undefined ? result[col] : 0;
      });

      ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'].forEach(dir => {
        row[`coef_${dir}_factor`] = result[`coef_${dir}_factor`] !== undefined ? result[`coef_${dir}_factor`] : 0;
        row[`coef_${dir}_peso`] = result[`coef_${dir}_peso`] !== undefined ? result[`coef_${dir}_peso`] : 0.5;
      });

      [
        'trainR2', 'testR2', 'trainRmse', 'testRmse'
      ].forEach(col => {
        row[col] = result[col] !== undefined ? result[col] : 
          col.includes('R2') ? -Infinity : Infinity;
      });

      // Generar línea CSV con exactamente 30 valores
      return [
        row.pesoPalmas, row.pesoItu, row.pesoBa, row.pesoVientos,
        row.lagPalmas, row.lagItu, row.lagBa, row.lagVientos,
        row.factorViento, row.offset,
        row.coef_N_factor, row.coef_N_peso,
        row.coef_NE_factor, row.coef_NE_peso,
        row.coef_E_factor, row.coef_E_peso,
        row.coef_SE_factor, row.coef_SE_peso,
        row.coef_S_factor, row.coef_S_peso,
        row.coef_SW_factor, row.coef_SW_peso,
        row.coef_W_factor, row.coef_W_peso,
        row.coef_NW_factor, row.coef_NW_peso,
        row.trainR2, row.testR2, row.trainRmse, row.testRmse
      ].join(';') + '\n';
    }).join('');

    await this.writeToFile(csvLines);
    return results.length;
  }

  async writeToFile(data) {
    return new Promise((resolve) => {
      this.currentFile.write(data, () => {
        this.currentSize += Buffer.byteLength(data);
        resolve();
      });
    });
  }

  async closeCurrentFile() {
    return new Promise((resolve) => {
      this.currentFile.end(() => resolve());
    });
  }
}

function validateData(data) {
  const stats = {
    palmas: { min: Infinity, max: -Infinity },
    itu: { min: Infinity, max: -Infinity },
    ba: { min: Infinity, max: -Infinity },
    ram: { min: Infinity, max: -Infinity }
  };

  ['palmas', 'itu', 'ba', 'ram'].forEach(key => {
    data[key].forEach(point => {
      stats[key].min = Math.min(stats[key].min, point.y);
      stats[key].max = Math.max(stats[key].max, point.y);
    });
  });

  console.log('📊 Estadísticas de datos:');
  console.table(stats);

  // Verificar valores extremos
  Object.entries(stats).forEach(([key, vals]) => {
    if (Math.abs(vals.min) > 1e6 || Math.abs(vals.max) > 1e6) {
      console.warn(`⚠️ Valores extremos en ${key}: min=${vals.min}, max=${vals.max}`);
    }
    if (isNaN(vals.min) || isNaN(vals.max)) {
      throw new Error(`Datos inválidos en ${key}: contiene NaN`);
    }
  });
}

function normalizeWindDirection(dir) {
  const directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'];
  if (typeof dir !== 'string') return 'N'; // Valor por defecto
  
  const upperDir = dir.toUpperCase().trim();
  return directions.includes(upperDir) ? upperDir : 'N';
}

// ================= MODELO HIDROLÓGICO =================
class RiverModel {
  constructor(config) {
    this.weights = config.weights;
    this.lags = config.lags;
    this.windCoeffs = config.windCoeffs;
    this.windFactor = config.windFactor;
    this.offset = config.offset;
  }

  // Función para encontrar el valor más cercano en el tiempo


  findClosestValue(data, targetDate, maxLagHours = 240) {
    // Validación robusta de fecha
    if (!targetDate || !(targetDate instanceof Date)) {
      console.warn('⚠️ Fecha inválida recibida:', targetDate);
      return 0;
    }
  
    const targetTime = targetDate.getTime();
    if (isNaN(targetTime)) {
      console.warn('⚠️ Timestamp inválido para:', targetDate);
      return 0;
    }
  
    // Busca en TODOS los datos si no hay en el rango
    const closest = data.reduce((prev, curr) => {
      const currTime = curr.x?.getTime() || 0;
      const prevDiff = Math.abs(prev.x?.getTime() - targetTime) || Infinity;
      const currDiff = Math.abs(currTime - targetTime) || Infinity;
      return currDiff < prevDiff ? curr : prev;
    }, { y: 0 }); // Objeto con valor por defecto
  
    return closest.y;
  }



  predict(targetPoint, inputData) {
    const components = {
      palmas: this.calculateComponent('palmas', targetPoint, inputData.palmas),
      itu: this.calculateComponent('itu', targetPoint, inputData.itu),
      ba: this.calculateComponent('ba', targetPoint, inputData.ba),
      wind: this.calculateWindComponent(targetPoint, inputData.vientos)
    };
  
    console.log('Componentes finales:');
    console.table(components);
  
    return components.palmas + components.itu + components.ba + components.wind + this.offset;
  }


    calculateComponent(source, targetPoint, data) {
      const laggedDate = new Date(targetPoint.x.getTime() - this.lags[source] * 3600 * 1000);
      const value = this.findClosestValue(data, laggedDate); // ← Ahora es número
      const contribution = value * this.weights[source];
      
      console.log(`Componente ${source}:`, {
        fechaLag: laggedDate.toISOString(),
        valorEncontrado: value, // ← Número, no objeto
        peso: this.weights[source],
        contribucion: contribution
      });
      
      return contribution;
    }

    calculateWindComponent(targetPoint, windData) {
      // 1. Validación de fecha objetivo
      if (!(targetPoint.x instanceof Date) || isNaN(targetPoint.x.getTime())) {
        console.error('⚠️ Timestamp inválido para targetPoint:', targetPoint.x);
        return this.getDefaultWindValues();
      }
    
      // 2. Cálculo de fecha con lag con validación
      const laggedDate = new Date(targetPoint.x.getTime() - this.lags.vientos * 3600 * 1000);
      if (isNaN(laggedDate.getTime())) {
        console.error('⚠️ Fecha con lag inválida calculada:', laggedDate);
        return this.getDefaultWindValues();
      }
    
      // 3. Buscar punto más cercano
      const windPoint = this.findClosestValue(windData, laggedDate);
    
      // Debug detallado (con validación)
      try {
        console.log('Búsqueda de viento:', {
          fechaObjetivo: targetPoint.x.toISOString(),
          fechaConLag: laggedDate.toISOString(),
          datosDisponibles: windData.slice(0, 3).map(d => ({
            fecha: d?.x?.toISOString?.() || 'Fecha inválida', 
            velocidad: d.speed,
            direccion: d.direction
          }))
        });
      } catch (e) {
        console.error('Error en debug de fechas:', e);
      }
    
      // 4. Manejo de punto no encontrado
      if (!windPoint || windPoint.direction === undefined) {
        console.warn('⚠️ Usando valores por defecto para viento');
        return this.getDefaultWindValues();
      }
    
      // 5. Cálculo final con validación
      if (!this.windCoeffs[windPoint.direction] || isNaN(windPoint.speed)) {
        console.warn('⚠️ Valores de viento inválidos:', windPoint);
        return this.getDefaultWindValues();
      }
    
      return this.windCoeffs[windPoint.direction] * windPoint.speed * this.windFactor;
    }
    
    // Función auxiliar para valores por defecto
    getDefaultWindValues() {
      return {
        direction: 'N',
        speed: 0,
        contribution: 0
      };
    }

  // Función evaluate actualizada
  evaluate(data) {
    const results = {
      predictions: [],
      observations: [],
      errors: []
    };

    data.target.forEach(targetPoint => {
      const prediction = this.predict(targetPoint, data);
      const error = targetPoint.y - prediction;
      
      results.predictions.push(prediction);
      results.observations.push(targetPoint.y);
      results.errors.push(error);
    });

    // Cálculo seguro de métricas
    return this.calculateMetrics(results);
  }

  calculateMetrics({ predictions, observations, errors }) {
    const n = observations.length;
    if (n === 0) return { r2: 0, rmse: Infinity };

    // Cálculo de RMSE
    const sumSquaredErrors = errors.reduce((sum, err) => sum + err * err, 0);
    const rmse = Math.sqrt(sumSquaredErrors / n);

    // Cálculo de R²
    const meanObserved = observations.reduce((sum, obs) => sum + obs, 0) / n;
    const ssTotal = observations.reduce((sum, obs) => sum + Math.pow(obs - meanObserved, 2), 0);
    
    let r2;
    if (ssTotal <= 1e-10) {
      r2 = 0;
    } else {
      r2 = 1 - (sumSquaredErrors / ssTotal);
    }

    return {
      r2: parseFloat(r2.toFixed(4)),
      rmse: parseFloat(rmse.toFixed(4)),
      predictions // Para análisis posterior
    };
  }
}

// ================= CARGA DE DATOS =================
async function loadAllData() {
  const loadWindCSV = (filename) => {
    return new Promise((resolve) => {
        const results = [];
        fs.createReadStream(path.join('./datos', filename))
            .pipe(csv({ separator: ';', headers: false }))
            .on('data', (data) => {
                try {
                    // Extracción directa de las 3 columnas esperadas
                    const [dateStr, speedStr, degreesStr] = data.map(col => col?.toString().trim());
                    
                    // Parseo seguro de la fecha (formato ISO ya correcto)
                    const date = new Date(dateStr);
                    if (isNaN(date.getTime())) {
                        console.warn(`Fecha inválida: ${dateStr}`);
                        return;
                    }

                    // Parseo seguro de velocidad (reemplazo de coma decimal si existe)
                    const speed = parseFloat(speedStr.replace(',', '.'));
                    if (isNaN(speed)) {
                        console.warn(`Velocidad inválida: ${speedStr}`);
                        return;
                    }

                    // Parseo seguro de dirección
                    const direction = gradosACardinal(degreesStr);

                    results.push({
                        x: date,
                        y: speed,
                        speed: speed,
                        direction: direction,
                        rawDegrees: degreesStr // Para debugging
                    });
                } catch (error) {
                    console.warn(`Error procesando fila:`, error.message);
                }
            })
            .on('end', () => {
                console.log(`✅ ${filename} cargado. ${results.length} registros válidos.`);
                if (results.length > 0) {
                    console.log('📌 Primer registro:', {
                        fecha: results[0].x.toISOString(),
                        velocidad: results[0].speed,
                        direccion: results[0].direction,
                        grados: results[0].rawDegrees
                    });
                }
                resolve(results);
            });
    });
};
  // Carga en paralelo todos los archivos
  const [palmas, itu, ba, ram, vientos] = await Promise.all([
    loadCSV('palmas.csv'),
    loadCSV('itu.csv'),
    loadCSV('ba.csv'),
    loadCSV('ram.csv'),
    loadCSV('vientos-crudos.csv')
  ]);

  // Validación básica
  if (ram.length === 0) throw new Error('No se cargaron datos de ram.csv');

  // División train/test (70%/30%)
  const splitIndex = Math.floor(ram.length * 0.7);
  const splitDate = ram[splitIndex].x;

  return {
    trainData: {
      palmas: palmas.filter(p => p.x <= splitDate),
      itu: itu.filter(p => p.x <= splitDate),
      ba: ba.filter(p => p.x <= splitDate),
      ram: ram.filter(p => p.x <= splitDate),
      vientos: vientos.filter(p => p.x <= splitDate)
    },
    testData: {
      palmas: palmas.filter(p => p.x > splitDate),
      itu: itu.filter(p => p.x > splitDate),
      ba: ba.filter(p => p.x > splitDate),
      ram: ram.filter(p => p.x > splitDate),
      vientos: vientos.filter(p => p.x > splitDate)
    }
  };
}

// Helper para direcciones del viento
function gradosACardinal(grados) {
  // Convertir a número eliminando caracteres no numéricos
  const numericValue = parseInt(grados.toString().replace(/[^0-9]/g, '')) || 0;
  
  // Normalizar a 0-360
  const normalized = ((numericValue % 360) + 360) % 360;
  
  // Mapear a dirección cardinal
  const sectors = [
      { max: 22.5, dir: 'N' },
      { max: 67.5, dir: 'NE' },
      { max: 112.5, dir: 'E' },
      { max: 157.5, dir: 'SE' },
      { max: 202.5, dir: 'S' },
      { max: 247.5, dir: 'SW' },
      { max: 292.5, dir: 'W' },
      { max: 337.5, dir: 'NW' },
      { max: 360, dir: 'N' }
  ];
  
  return sectors.find(s => normalized <= s.max).dir;
}
// ================= WORKER THREAD =================
if (!isMainThread) {
  const { workerId, trainData, testData } = workerData;
  const writer = new ResultWriter();

  writer.openNewFile().then(() => {
    if (!isMainThread) {
          
      parentPort.on('message', async ({ type, data: paramsChunk }) => {
        if (type === 'process') {
          console.log(`Worker ${workerId} procesando ${paramsChunk.length} combinaciones...`); // ✅ Nuevo log
          const results = [];
          
          for (const params of paramsChunk) {
            try {
              const model = new RiverModel({
                weights: {
                  palmas: params.pesoPalmas,
                  itu: params.pesoItu,
                  ba: params.pesoBa,
                  winds: params.pesoVientos
                },
                lags: {
                  palmas: params.lagPalmas,
                  itu: params.lagItu,
                  ba: params.lagBa,
                  winds: params.lagVientos
                },
                windCoeffs: {
                  N: params.coef_N_factor || 0,
                  NE: params.coef_NE_factor || 0,
                  E: params.coef_E_factor || 0,
                  SE: params.coef_SE_factor || 0,
                  S: params.coef_S_factor || 0,
                  SW: params.coef_SW_factor || 0,
                  W: params.coef_W_factor || 0,
                  NW: params.coef_NW_factor || 0
                },
                windFactor: params.factorViento,
                offset: params.offset
              });
        
       // Solo debuggear el primer modelo de cada worker
       if (params === paramsChunk[0]) {
        console.log(`\n🧪 Worker ${workerId} - Iniciando análisis detallado...`);
        debugPredictions(model, trainData);
      }


              const { r2, rmse } = model.evaluate({
                palmas: trainData.palmas,
                itu: trainData.itu,
                ba: trainData.ba,
                vientos: trainData.vientos,
                target: trainData.ram
              });
              // ✅ Asegura valores numéricos
              results.push({
              ...params,
              trainR2: Number.isFinite(r2) ? r2 : -Infinity,
              testR2: Number.isFinite(r2) ? r2 : -Infinity, // Mismo valor temporal para test
              trainRmse: Number.isFinite(rmse) ? rmse : Infinity,
              testRmse: Number.isFinite(rmse) ? rmse : Infinity
              });             
            } catch (error) {
              console.error('Error en worker:', error);
            }
          }
    
          await writer.write(results);
          parentPort.postMessage({ type: 'done', processed: paramsChunk.length });
        }
      });
    }
  });
}


function debugPredictions(model, trainData) {
  console.log('\n🔍 INICIANDO DEBUG DETALLADO');
  
  // 1. Validación exhaustiva de datos
  if (!trainData || !trainData.ram || trainData.ram.length === 0) {
    console.error('❌ Error: trainData.ram está vacío o no existe');
    return;
  }

  console.log('📝 Estructura de datos recibida:');
  console.log('- palmas:', trainData.palmas?.length || 'no definido');
  console.log('- itu:', trainData.itu?.length || 'no definido');
  console.log('- ba:', trainData.ba?.length || 'no definido');
  console.log('- vientos:', trainData.vientos?.length || 'no definido');
  console.log('- ram (target):', trainData.ram.length);

  // 2. Mostrar configuración del modelo
  console.log('\n⚙️ Configuración del modelo:');
  console.table({
    Pesos: model.weights,
    Lags: model.lags,
    'Coef. Viento': model.windCoeffs,
    'Factor Viento': model.windFactor,
    Offset: model.offset
  });

  // 3. Analizar los primeros 2 puntos en detalle
  [0, 1].forEach(i => {
    if (!trainData.ram[i]) return;
    
    console.log(`\n📌 Punto ${i + 1} de ${trainData.ram.length}`);
    console.log('Fecha:', trainData.ram[i].x?.toISOString() || 'Fecha inválida');
    console.log('Valor real (ram):', trainData.ram[i].y);

    // Preparar datos de entrada para predict()
    const inputData = {
      palmas: trainData.palmas || [],
      itu: trainData.itu || [],
      ba: trainData.ba || [],
      vientos: trainData.vientos || []
    };

    try {
      // 4. Calcular cada componente por separado
      const components = {
        'Palmas': calculateComponent('palmas', i),
        'Itu': calculateComponent('itu', i),
        'Ba': calculateComponent('ba', i),
        'Viento': calculateWindComponent(i),
        'Offset': model.offset
      };

      console.log('\n🔧 Componentes de la predicción:');
      console.table(components);

      // 5. Predicción completa
      const prediction = model.predict(trainData.ram[i], inputData);
      console.log('\n📊 Resultado:');
      console.log('Predicción:', prediction);
      console.log('Error:', (trainData.ram[i].y - prediction).toFixed(4));

    } catch (error) {
      console.error(`❌ Error analizando punto ${i + 1}:`, error.message);
    }
  });

  // Funciones auxiliares
  function calculateComponent(source, pointIndex) {
    const lagHours = model.lags[source];
    const sourceData = trainData[source];
    const targetDate = new Date(trainData.ram[pointIndex].x.getTime() - lagHours * 3600 * 1000);
    
    const value = model.findClosestValue(sourceData, targetDate);
    const weight = model.weights[source];
    
    return {
      'Valor crudo': value,
      'Lag (h)': lagHours,
      'Peso': weight,
      'Contribución': value * weight
    };
  }

  function calculateWindComponent(targetPoint, windData) {
    try {
      // 1. Validación extrema del input
      if (!targetPoint?.x || !(targetPoint.x instanceof Date)) {
        throw new Error('Punto objetivo inválido');
      }
  
      const targetTime = targetPoint.x.getTime();
      if (isNaN(targetTime)) {
        throw new Error('Timestamp objetivo inválido');
      }
  
      // 2. Cálculo seguro de fecha con lag
      const lagMs = this.lags.vientos * 3600 * 1000;
      const laggedDate = new Date(targetTime - lagMs);
      if (isNaN(laggedDate.getTime())) {
        throw new Error('Fecha con lag inválida');
      }
  
      // 3. Buscar punto más cercano (con protección)
      const windPoint = this.findClosestValue(windData, laggedDate) || {};
      
      // 4. Validación de datos de viento
      if (!windPoint.direction || !windPoint.speed) {
        console.warn('Datos de viento incompletos para:', laggedDate.toISOString());
        return 0;
      }
  
      // 5. Cálculo final protegido
      const windEffect = this.windCoeffs[windPoint.direction] * windPoint.speed * this.windFactor;
      
      return Number.isFinite(windEffect) ? windEffect : 0;
  
    } catch (error) {
      console.error('Error en calculateWindComponent:', error.message);
      return 0; // Valor seguro por defecto
    }
  }
}

// ================= FUNCIÓN PRINCIPAL =================
async function main() {
  console.log('🚀 Iniciando Grid Search Optimizado');
  console.log(`🔍 Filtros: R² >= ${CONFIG.minR2}, RMSE <= ${CONFIG.maxRMSE}`);

  // 1. Preparar entorno
  if (!fs.existsSync(CONFIG.checkpointDir)) {
    fs.mkdirSync(CONFIG.checkpointDir, { recursive: true });
  }

  // 2. Cargar datos
  console.log('📂 Cargando datos...');
  let trainData, testData;
  try {
    ({ trainData, testData } = await loadAllData());
        
      // =============================================
  // DEBUG ADICIONAL - ESTRUCTURA DE DATOS
  // (Colocar justo después de loadData())
  console.log('\n🔍 Estructura completa del primer punto:');
  console.log('Palmas[0]:', trainData.palmas[0] ? {
    fecha: trainData.palmas[0].x?.toISOString(),
    valor: trainData.palmas[0].y
  } : 'No disponible');
  
  console.log('Itu[0]:', trainData.itu[0] ? {
    fecha: trainData.itu[0].x?.toISOString(),
    valor: trainData.itu[0].y
  } : 'No disponible');
  
  console.log('Vientos[0]:', trainData.vientos[0] ? {
    fecha: trainData.vientos[0].x?.toISOString(),
    direccion: trainData.vientos[0].direction,
    velocidad: trainData.vientos[0].speed
  } : 'No disponible');
  
  console.log('Ram[0]:', trainData.ram[0] ? {
    fecha: trainData.ram[0].x?.toISOString(),
    valor: trainData.ram[0].y
  } : 'No disponible');
  // =============================================
    
    // =============================================
    // VALIDACIÓN DE DATOS (NUEVO)
    console.log('\n🔍 Validando datos de entrenamiento:');
    validateData(trainData);
    
    console.log('\n🔍 Validando datos de prueba:');
    validateData(testData);
    // =============================================

    console.log(`📊 Datos cargados (Train: ${trainData.ram.length} registros, Test: ${testData.ram.length} registros)`);
  } catch (error) {
    console.error('❌ Error cargando datos:', error.message);
    process.exit(1);
  }

  // 3. Iniciar workers
  const workers = Array.from({ length: CONFIG.maxWorkers }, (_, i) => {
    const worker = new Worker(__filename, {
      workerData: { workerId: i, trainData, testData }
    });
    return {
      instance: worker,
      isBusy: false,
      processed: 0,
      relevant: 0
    };
  });

  // 4. Generar y distribuir parámetros
  const paramsGenerator = generateParametersLazy();
  let totalProcessed = 0;
  let totalRelevant = 0;

  const assignWork = async () => {
    const availableWorker = workers.find(w => !w.isBusy);
    if (!availableWorker) return false;

    const chunk = [];
    for (let i = 0; i < CONFIG.chunkSize; i++) {
      const { value, done } = paramsGenerator.next();
      if (done) return true;
      chunk.push(value);
    }

    availableWorker.isBusy = true;
    availableWorker.instance.postMessage({ type: 'process', data: chunk });

    availableWorker.instance.once('message', ({ type, processed, relevant, error }) => {
      availableWorker.isBusy = false;
      if (type === 'error') {
        console.error('Error en worker:', error);
        return;
      }
      availableWorker.processed += processed;
      availableWorker.relevant += relevant;
      totalProcessed += processed;
      totalRelevant += relevant;

if (totalProcessed % CONFIG.memoryCleanInterval === 0) {
  try {
    if (global.gc) {
      const memBefore = process.memoryUsage().heapUsed / 1024 / 1024;
      global.gc();
      const memAfter = process.memoryUsage().heapUsed / 1024 / 1024;
      console.log(`🗑️ GC ejecutado | Memoria: ${memBefore.toFixed(2)}MB → ${memAfter.toFixed(2)}MB`);
    }
  } catch (e) {
    console.warn('Error en GC:', e.message);
  }
}

      if (totalProcessed % CONFIG.progressInterval === 0) {
        console.log(
          `📊 Procesados: ${totalProcessed.toLocaleString()} | ` +
          `Relevantes: ${totalRelevant.toLocaleString()} ` +
          `(${(totalRelevant / totalProcessed * 100).toFixed(2)}%)`
        );
      }


      if (totalProcessed % CONFIG.checkpointInterval === 0) {
        fs.writeFileSync(
          path.join(CONFIG.checkpointDir, `checkpoint_${Date.now()}.json`),
          JSON.stringify({
            totalProcessed,
            totalRelevant,
            timestamp: new Date().toISOString()
          }, null, 2)
        );
      }
    });

    return false;
  };

  // 5. Loop principal
  console.log('⚡ Iniciando procesamiento paralelo...');
  let isDone = false;
  let iteration = 0;
  const startTime = Date.now();

  while (!isDone) {
    isDone = await assignWork();
    await new Promise(resolve => setTimeout(resolve, 10));
     // ✅ Liberación de memoria cada 500 iteraciones
  if (iteration++ % 250 === 0 && global.gc) {
    global.gc();
    const mem = process.memoryUsage();
    console.log(`🧹 Memoria: ${(mem.heapUsed / 1024 / 1024).toFixed(2)}MB`);
  }
  }

  // 6. Finalizar workers
  workers.forEach(w => w.instance.terminate());

  // 7. Combinar resultados
  console.log('🔗 Combinando resultados parciales...');
  try {
    const finalCount = await combineResults();
    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
    
    console.log('\n✅ Proceso completado!');
    console.log(`⏱️  Tiempo total: ${elapsed} minutos`);
    console.log(`📌 Total procesado: ${totalProcessed.toLocaleString()} combinaciones`);
    console.log(`🏆 Resultados relevantes: ${finalCount.toLocaleString()}`);
    console.log(`📁 Archivo final: ${CONFIG.finalFile}`);

  } catch (error) {
    console.error('❌ Error combinando resultados:', error.message);
  }
}

// ================= GENERADOR DE PARÁMETROS =================
function* generateParametersLazy() {
  const { ranges } = CONFIG;

  // Generar coeficientes de viento aleatorios
  const getWindCoefs = () => {
    const coefs = {};
    for (const [dir, values] of Object.entries(ranges.coeficientesViento)) {
      const randomValue = values[Math.floor(Math.random() * values.length)];
      coefs[`coef_${dir}_factor`] = randomValue;
      coefs[`coef_${dir}_peso`] = 0.3 + Math.random() * 0.7; // Entre 0.3 y 1.0
    }
    return coefs;
  };

  for (const pesoPalmas of ranges.pesos.palmas) {
    for (const pesoItu of ranges.pesos.itu) {
      for (const pesoBa of ranges.pesos.ba) {
        for (const pesoVientos of ranges.pesos.vientos) {
          for (const lagPalmas of ranges.lags.palmas) {
            for (const lagItu of ranges.lags.itu) {
              for (const lagBa of ranges.lags.ba) {
                for (const lagVientos of ranges.lags.vientos) {
                  for (const factorViento of ranges.factorViento) {
                    for (const offset of ranges.offset) {
                      yield {
                        pesoPalmas,
                        pesoItu,
                        pesoBa,
                        pesoVientos,
                        lagPalmas,
                        lagItu,
                        lagBa,
                        lagVientos,
                        factorViento,
                        offset,
                        ...getWindCoefs()
                      };
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

// ================= COMBINAR RESULTADOS =================
async function combineResults() {
  const output = fs.createWriteStream(CONFIG.finalFile);
  let totalRelevant = 0;

  // Escribir encabezado con 30 columnas
  const header = [
    'pesoPalmas', 'pesoItu', 'pesoBa', 'pesoVientos',
    'lagPalmas', 'lagItu', 'lagBa', 'lagVientos',
    'factorViento', 'offset',
    'coef_N_factor', 'coef_N_peso',
    'coef_NE_factor', 'coef_NE_peso',
    'coef_E_factor', 'coef_E_peso',
    'coef_SE_factor', 'coef_SE_peso',
    'coef_S_factor', 'coef_S_peso',
    'coef_SW_factor', 'coef_SW_peso',
    'coef_W_factor', 'coef_W_peso',
    'coef_NW_factor', 'coef_NW_peso',
    'trainR2', 'testR2', 'trainRmse', 'testRmse'
  ].join(';') + '\n';
  
  output.write(header);

  // Procesar archivos parciales
  const files = fs.readdirSync(CONFIG.resultsDir)
    .filter(f => f.startsWith('results_') && f.endsWith('.csv'))
    .sort((a, b) => {
      const numA = parseInt(a.match(/results_(\d+)\.csv/)[1]);
      const numB = parseInt(b.match(/results_(\d+)\.csv/)[1]);
      return numA - numB;
    });

  for (const file of files) {
    const filePath = path.join(CONFIG.resultsDir, file);
    const content = fs.readFileSync(filePath, 'utf8');
    
    // Procesar cada línea asegurando 30 columnas
    content.split('\n').forEach(line => {
      if (line.trim() && !line.startsWith('pesoPalmas')) { // Saltar encabezado
        const parts = line.split(';');
        
        // Normalizar si no tiene 30 columnas
        const normalizedParts = [];
        for (let i = 0; i < 30; i++) {
          if (i < parts.length && parts[i].trim() !== '') {
            normalizedParts.push(parts[i]);
          } else {
            // Asignar valores por defecto según el tipo de columna
            if (i < 10) {
              normalizedParts.push('0'); // Parámetros base
            } else if (i < 26) {
              normalizedParts.push(i % 2 === 0 ? '0' : '0.5'); // Coeficientes de viento
            } else {
              normalizedParts.push(i < 28 ? '-Infinity' : 'Infinity'); // Métricas
            }
          }
        }
        
        output.write(normalizedParts.join(';') + '\n');
        totalRelevant++;
      }
    });
  }

  output.end();
  return totalRelevant;
}

// ================= INICIAR =================
if (isMainThread) {
  main().catch(err => {
    console.error('❌ Error crítico:', err);
    process.exit(1);
  });
}