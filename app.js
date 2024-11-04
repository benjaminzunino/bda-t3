const cassandra = require('cassandra-driver');
const { v4: uuidv4 } = require('uuid');

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1'
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
  const query = `
    CREATE TABLE IF NOT EXISTS estampilla_country_year (
      id UUID PRIMARY KEY,
      titulo TEXT,
      country TEXT,
      year INT
    );
  `;
  try {
    await client.execute(query);
    console.log('Tabla "estampilla_country_year" creada');
  } catch (err) {
    console.error('Error al crear la tabla', err);
  }
}

async function agregarEstampilla(titulo, country, year) {
  const query = 'INSERT INTO estampilla_country_year (id, titulo, country, year) VALUES (?, ?, ?, ?)';
  const params = [uuidv4(), titulo, country, year];
  try {
    await client.execute(query, params, { prepare: true });
    console.log('Estampilla agregada');
  } catch (err) {
    console.error('Error al agregar la estampilla', err);
  }
}

async function menu(){
    await conectar();
    await crear_keyspace();
    await crear_tablas();
    await agregarEstampilla('Estampilla de ejemplo', 'Argentina', 2023);
}

menu();