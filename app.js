const cassandra = require('cassandra-driver');
const { v4: uuidv4 } = require('uuid');
const readline = require('readline');

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1'
});

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

async function conectar() {
  try {
    await client.connect();
    console.log('Conectado a Cassandra');
  } catch (err) {
    console.error('Error al conectar a Cassandra', err);
  }
}

async function crear_keyspace() {
  const query = `
    CREATE KEYSPACE IF NOT EXISTS estampillas WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
    };
  `;
  try {
    await client.execute(query);
    console.log('Keyspace "estampillas" creado');
  } catch (err) {
    console.error('Error al crear el keyspace', err);
  }
  try {
    await client.execute('USE estampillas');
    console.log('Usando el keyspace "estampillas"');
  } catch (err) {
    console.error('Error al usar el keyspace', err);
  }
}

async function crear_tablas() {
  try {
    // Elimina la tabla si ya existe
    await client.execute('DROP TABLE IF EXISTS estampilla_country_year');
    console.log('Tabla "estampilla_country_year" eliminada');

    // Crea la tabla con la nueva columna 'stamp_id'
    const query = `
      CREATE TABLE IF NOT EXISTS estampilla_country_year (
        id UUID PRIMARY KEY,
        stamp_id INT,
        titulo TEXT,
        country TEXT,
        year INT,
        serie TEXT,
        design TEXT,
        face_value FLOAT,
        condition TEXT,
        status TEXT,
        seller TEXT,
        transaction_history LIST<TEXT>,
        tags SET<TEXT>,
        time_value MAP<TIMESTAMP, FLOAT>
      );
    `;
    await client.execute(query);
    console.log('Tabla "estampilla_country_year" creada');
  } catch (err) {
    console.error('Error al crear la tabla', err);
  }
}



// PREGUNTAS //

async function agregarEstampillaCompleta(
  stamp_id,
  titulo,
  country,
  year,
  serie,
  design,
  face_value,
  condition,
  status,
  seller,
  transaction_history,
  tags,
  time_value
) {
  const query = `
    INSERT INTO estampilla_country_year (id, stamp_id, titulo, country, year, serie, design, face_value, condition, status, seller, transaction_history, tags, time_value)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;
  const params = [
    uuidv4(),
    stamp_id,
    titulo,
    country,
    year,
    serie,
    design,
    face_value,
    condition,
    status,
    seller,
    transaction_history,
    Array.from(tags),  // Convierte el Set en un Array
    time_value
  ];
  try {
    await client.execute(query, params, { prepare: true });
    console.log("Estampilla completa agregada");
  } catch (err) {
    console.error("Error al agregar la estampilla completa", err);
  }
}

async function preguntarDatosEstampilla() {
  return new Promise((resolve) => {
    const datos = {};

    rl.question('Ingrese el título de la estampilla: ', (titulo) => {
      datos.titulo = titulo;
      rl.question('Ingrese el país de emisión: ', (country) => {
        datos.country = country;
        rl.question('Ingrese el año de emisión: ', (year) => {
          datos.year = parseInt(year);
          rl.question('Ingrese la serie: ', (serie) => {
            datos.serie = serie;
            rl.question('Ingrese el diseño: ', (design) => {
              datos.design = design;
              rl.question('Ingrese el valor nominal (face value): ', (face_value) => {
                datos.face_value = parseFloat(face_value);
                rl.question('Ingrese la condición (nuevo, usado, dañado): ', (condition) => {
                  datos.condition = condition;
                  rl.question('Ingrese el estado (disponible, vendido, reservado): ', (status) => {
                    datos.status = status;
                    rl.question('Ingrese el nombre del vendedor: ', (seller) => {
                      datos.seller = seller;
                      rl.question('Ingrese las transacciones (separadas por comas): ', (transaction_history) => {
                        datos.transaction_history = transaction_history.split(',').map(item => item.trim());
                        rl.question('Ingrese las etiquetas (tags) separadas por comas: ', (tags) => {
                          datos.tags = new Set(tags.split(',').map(item => item.trim()));
                          rl.question('Ingrese el valor temporal (fecha y valor separados por comas, múltiples pares separados por punto y coma): ', (time_value) => {
                            datos.time_value = new Map(time_value.split(';').map(pair => {
                              const [date, value] = pair.split(',').map(item => item.trim());
                              return [new Date(date), parseFloat(value)];
                            }));
                            resolve(datos);
                          });
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
}

async function buscarEstampillas(country, year, tags = []) {
  let query = 'SELECT titulo, stamp_id, status, time_value FROM estampilla_country_year WHERE country = ? AND year = ?';
  const params = [country, year];

  if (tags.length > 0) {
    query += ' AND tags CONTAINS ?';
    params.push(tags[0]);
    if (tags.length > 1) {
      for (let i = 1; i < tags.length; i++) {
        query += ' AND tags CONTAINS ?';
        params.push(tags[i]);
      }
    }
  }

  // Coloca ALLOW FILTERING al final de la consulta
  query += ' ALLOW FILTERING';

  try {
    const result = await client.execute(query, params, { prepare: true });
    console.log('Estampillas encontradas:', result.rows);
  } catch (err) {
    console.error('Error al buscar estampillas', err);
  }
}

async function actualizarTimeValue(stamp_id, date, value) {
  const queryGetId = 'SELECT id FROM estampilla_country_year WHERE stamp_id = ? ALLOW FILTERING';
  const paramsGetId = [stamp_id];
  
  try {
    // Primero obtenemos el id usando el stamp_id
    const result = await client.execute(queryGetId, paramsGetId, { prepare: true });

    if (result.rows.length > 0) {
      const id = result.rows[0].id;

      // Luego actualizamos el time_value utilizando el id encontrado
      const queryUpdate = 'UPDATE estampilla_country_year SET time_value[?] = ? WHERE id = ?';
      const paramsUpdate = [date, value, id];

      // Ejecutamos la consulta de actualización
      await client.execute(queryUpdate, paramsUpdate, { prepare: true });
      console.log('Time value actualizado');
    } else {
      console.log('No se encontró ningún registro con el stamp_id proporcionado');
    }
  } catch (err) {
    console.error('Error al actualizar time value', err);
  }
}

async function agregarTransaccion(stamp_id, transaccion) {
  // Primero obtenemos el id
  const selectQuery = 'SELECT id FROM estampilla_country_year WHERE stamp_id = ? ALLOW FILTERING';
  try {
    const result = await client.execute(selectQuery, [stamp_id], { prepare: true });

    if (result.rows.length > 0) {
      const id = result.rows[0].id;  // Obtenemos el id de la primera fila

      // Segundo paso: realizar la actualización utilizando el id obtenido
      const updateQuery = 'UPDATE estampilla_country_year SET transaction_history = transaction_history + ? WHERE id = ?';
      await client.execute(updateQuery, [[transaccion], id], { prepare: true });

      console.log('Transacción agregada');
    } else {
      console.error('No se encontró el id para el stamp_id proporcionado');
    }
  } catch (err) {
    console.error('Error al agregar transacción', err);
  }
}

async function comprarEstampilla(titulo, sellerId) {
  const selectQuery = 'SELECT id, status FROM estampilla_country_year WHERE titulo = ? AND seller = ? ALLOW FILTERING';
  const updateQuery = `
    BEGIN BATCH
    UPDATE estampilla_country_year SET status = 'vendido' WHERE id = ?;
    UPDATE estampilla_country_year SET seller = 'D' WHERE id = ?;
    UPDATE estampilla_country_year SET transaction_history = transaction_history + ['compra'] WHERE id = ?;
    APPLY BATCH;
  `;
  try {
    const result = await client.execute(selectQuery, [titulo, sellerId], { prepare: true });
    if (result.rowLength > 0 && result.rows[0].status === 'disponible') {
      const id = result.rows[0].id;
      await client.execute(updateQuery, [id, id, id], { prepare: true });
      console.log('Compra realizada');
    } else {
      console.log('Estampilla no disponible para compra');
    }
  } catch (err) {
    console.error('Error al comprar estampilla', err);
  }
}

async function crearVistaMaterializada() {
  const query = `
    CREATE MATERIALIZED VIEW IF NOT EXISTS estampilla_por_condicion AS
    SELECT id, titulo, condition, year, face_value
    FROM estampilla_country_year
    WHERE condition IS NOT NULL AND year IS NOT NULL AND face_value IS NOT NULL
    PRIMARY KEY (condition, year, face_value, id)
    WITH CLUSTERING ORDER BY (year ASC, face_value DESC);
  `;
  try {
    await client.execute(query);
    console.log('Vista materializada creada');
  } catch (err) {
    console.error('Error al crear la vista materializada', err);
  }
}

async function buscarEstampillaMasBarata(status, startYear, endYear) {
  const query = `
    SELECT titulo, id, face_value FROM estampilla_country_year
    WHERE status = ? AND year >= ? AND year <= ?
    LIMIT 1
    ALLOW FILTERING
  `;


  const params = [status, startYear, endYear];
  try {
    const result = await client.execute(query, params, { prepare: true });
    console.log('Estampilla más barata:', result.rows[0]);
  } catch (err) {
    console.error('Error al buscar la estampilla más barata', err);
  }
}










async function encontrarEstampillaMasCara(condition, year) {
  const query = `
    SELECT titulo, id, face_value FROM estampilla_country_year
    WHERE condition = ? AND year = ?
    ORDER BY face_value DESC LIMIT 1
  `;
  const params = [condition, year];
  try {
    const result = await client.execute(query, params, { prepare: true });
    console.log('Estampilla más cara:', result.rows[0]);
  } catch (err) {
    console.error('Error al encontrar la estampilla más cara', err);
  }
}

async function consultarHistorialTransacciones(id) {
  const query = 'SELECT transaction_history FROM estampilla_country_year WHERE id = ?';
  const params = [id];
  try {
    const result = await client.execute(query, params, { prepare: true });
    console.log('Historial de transacciones:', result.rows[0].transaction_history);
  } catch (err) {
    console.error('Error al consultar el historial de transacciones', err);
  }
}

async function verificarEstampillasVendedor(seller) {
  const query = 'SELECT titulo, id FROM estampilla_country_year WHERE seller = ?';
  const params = [seller];
  try {
    const result = await client.execute(query, params, { prepare: true });
    console.log('Estampillas del vendedor:', result.rows);
  } catch (err) {
    console.error('Error al verificar las estampillas del vendedor', err);
  }
}










async function mostrarMenu() {
  console.log(`
    Selecciona una opción:
    1. Agregar estampilla
    2. Buscar estampillas por país y año
    3. Actualizar valor temporal de una estampilla
    4. Agregar transacción a una estampilla
    5. Comprar estampilla
    6. Crear vista materializada
    7. Buscar estampilla más barata por estado y rango de años
    8. Salir
  `);

  rl.question('Ingresa el número de la opción: ', async (opcion) => {
    switch (opcion) {
      case '1':
        await agregarDatos();
        break;
      case '2':
        await buscarEstampillas('Argentina', 2022, ['rara']);
        break;
      case '3':
        await actualizarTimeValue(2, new Date(), 12.0);
        break;
      case '4':
        await agregarTransaccion(2, 'venta');
        break;
      case '5':
        await comprarEstampilla('Estampilla de ejemplo A', 'Vendedor A');
        break;
      case '6':
        await crearVistaMaterializada();
        break;
      case '7':
        await buscarEstampillaMasBarata('disponible', 2020, 2023);
        break;
      case '8':
        rl.close();
        return;
      default:
        console.log('Opción no válida');
    }
    mostrarMenu();
  });
}

async function agregarDatos() {
  // Ejemplo 1: Estampilla en buen estado y disponible
  await agregarEstampillaCompleta(
    1, // stamp_id
    "Estampilla de ejemplo A",
    "Chile",
    2021,
    "Serie Patria",
    "Diseño de prócer",
    15.0,
    "nuevo",
    "disponible",
    "Vendedor A",
    ["creación", "primera venta"],
    new Set(["histórica", "rara"]),
    new Map([[new Date("2023-01-01"), 15.0]])
  );

  // Ejemplo 2: Estampilla en estado usado y vendida
  await agregarEstampillaCompleta(
    2, // stamp_id
    "Estampilla de ejemplo B",
    "Argentina",
    2022,
    "Serie Bicentenario",
    "Diseño de conmemoración",
    20.0,
    "usado",
    "vendido",
    "Vendedor B",
    ["venta", "reventa"],
    new Set(["colección", "rara"]),
    new Map([[new Date("2023-01-15"), 20.0], [new Date("2023-06-01"), 18.0]])
  );

  // Ejemplo 3: Estampilla en estado dañado y reservado
  await agregarEstampillaCompleta(
    3, // stamp_id
    "Estampilla de ejemplo C",
    "Brasil",
    2020,
    "Serie Fauna",
    "Diseño de animales",
    5.0,
    "dañado",
    "reservado",
    "Vendedor C",
    ["compra", "reserva"],
    new Set(["naturaleza", "económica"]),
    new Map([[new Date("2023-02-01"), 5.0], [new Date("2023-07-01"), 4.5]])
  );

  // Ejemplo 4: Estampilla en excelente estado y disponible
  await agregarEstampillaCompleta(
    4, // stamp_id
    "Estampilla de ejemplo D",
    "Perú",
    2023,
    "Serie Cultura",
    "Diseño cultural",
    30.0,
    "nuevo",
    "disponible",
    "Vendedor D",
    ["creación"],
    new Set(["antigua", "valiosa"]),
    new Map([[new Date("2023-03-01"), 30.0]])
  );

  // Ejemplo 5: Estampilla en estado nuevo y disponible
  await agregarEstampillaCompleta(
    5, // stamp_id
    "Estampilla de ejemplo E",
    "México",
    2021,
    "Serie Revolución",
    "Diseño histórico",
    25.0,
    "nuevo",
    "disponible",
    "Vendedor E",
    ["creación", "compra"],
    new Set(["histórica", "coleccionable"]),
    new Map([[new Date("2023-04-01"), 25.0]])
  );

  // Ejemplo 6: Estampilla en estado usado y vendida
  await agregarEstampillaCompleta(
    6, // stamp_id
    "Estampilla de ejemplo F",
    "Colombia",
    2019,
    "Serie Flora",
    "Diseño de flores",
    8.0,
    "usado",
    "vendido",
    "Vendedor F",
    ["compra", "venta", "reventa"],
    new Set(["naturaleza", "colección"]),
    new Map([[new Date("2023-05-01"), 8.0], [new Date("2023-07-01"), 7.5]])
  );

  // Ejemplo 7: Estampilla en estado dañado y reservado
  await agregarEstampillaCompleta(
    7, // stamp_id
    "Estampilla de ejemplo G",
    "Chile",
    2018,
    "Serie Animales",
    "Diseño de fauna",
    3.5,
    "dañado",
    "reservado",
    "Vendedor G",
    ["reserva", "intercambio"],
    new Set(["fauna", "económica"]),
    new Map([[new Date("2023-02-01"), 3.5], [new Date("2023-06-01"), 3.2]])
  );

  // Ejemplo 8: Estampilla en estado nuevo y disponible
  await agregarEstampillaCompleta(
    8, // stamp_id
    "Estampilla de ejemplo H",
    "Uruguay",
    2022,
    "Serie Fútbol",
    "Diseño deportivo",
    18.0,
    "nuevo",
    "disponible",
    "Vendedor H",
    ["creación", "colección"],
    new Set(["deporte", "valiosa"]),
    new Map([[new Date("2023-05-10"), 18.0]])
  );

  // Ejemplo 9: Estampilla en estado usado y vendido
  await agregarEstampillaCompleta(
    9, // stamp_id
    "Estampilla de ejemplo I",
    "Argentina",
    2020,
    "Serie Maravillas Naturales",
    "Diseño de montañas",
    10.0,
    "usado",
    "vendido",
    "Vendedor I",
    ["compra", "venta"],
    new Set(["naturaleza", "coleccionable"]),
    new Map([[new Date("2023-01-20"), 10.0], [new Date("2023-06-20"), 9.0]])
  );

  // Ejemplo 10: Estampilla en estado nuevo y disponible
  await agregarEstampillaCompleta(
    10, // stamp_id
    "Estampilla de ejemplo J",
    "Paraguay",
    2023,
    "Serie Monumentos",
    "Diseño arquitectónico",
    12.5,
    "nuevo",
    "disponible",
    "Vendedor J",
    ["creación"],
    new Set(["arquitectura", "rara"]),
    new Map([[new Date("2023-09-01"), 12.5]])
  );

  // Ejemplo 11: Estampilla en estado nuevo y disponible
  await agregarEstampillaCompleta(
    11, // stamp_id
    "Estampilla de ejemplo K",
    "Perú",
    2017,
    "Serie Tradiciones",
    "Diseño cultural",
    14.0,
    "nuevo",
    "disponible",
    "Vendedor K",
    ["colección"],
    new Set(["cultura", "antigua"]),
    new Map([[new Date("2023-02-15"), 14.0]])
  );

  // Ejemplo 12: Estampilla en estado dañado y reservado
  await agregarEstampillaCompleta(
    12, // stamp_id
    "Estampilla de ejemplo L",
    "Venezuela",
    2021,
    "Serie Paisajes",
    "Diseño de costa",
    6.0,
    "dañado",
    "reservado",
    "Vendedor L",
    ["compra", "reserva"],
    new Set(["paisajes", "económica"]),
    new Map([[new Date("2023-03-01"), 6.0], [new Date("2023-08-01"), 5.5]])
  );

  // Ejemplo 13: Estampilla en estado usado y vendida
  await agregarEstampillaCompleta(
    13, // stamp_id
    "Estampilla de ejemplo M",
    "Bolivia",
    2021,
    "Serie Carnaval",
    "Diseño de fiesta",
    9.0,
    "usado",
    "vendido",
    "Vendedor M",
    ["reventa", "intercambio"],
    new Set(["festividad", "cultura"]),
    new Map([[new Date("2023-06-15"), 9.0]])
  );

  // Ejemplo 14: Estampilla en estado nuevo y disponible
  await agregarEstampillaCompleta(
    14, // stamp_id
    "Estampilla de ejemplo N",
    "Ecuador",
    2020,
    "Serie Historia",
    "Diseño de figuras históricas",
    20.0,
    "nuevo",
    "disponible",
    "Vendedor N",
    ["creación", "colección"],
    new Set(["histórica", "valiosa"]),
    new Map([[new Date("2023-04-10"), 20.0]])
  );

  // Ejemplo 15: Estampilla en estado usado y vendida
  await agregarEstampillaCompleta(
    15,
    "Estampilla de ejemplo O",
    "Brasil",
    2016,
    "Serie Amazonas",
    "Diseño de flora y fauna",
    13.0,
    "usado",
    "vendido",
    "Vendedor O",
    ["venta", "reventa"],
    new Set(["naturaleza", "antigua"]),
    new Map([[new Date("2023-02-05"), 13.0], [new Date("2023-08-10"), 12.5]])
  );

}

async function menu() {
  await conectar();
  await crear_keyspace();
  await crear_tablas();

  mostrarMenu();
}

menu();
