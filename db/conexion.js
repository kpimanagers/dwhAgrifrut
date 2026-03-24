require('dotenv').config();
sql = require('mssql');

//Conexion al Origen
const sqlConfig = {
    user: process.env.BBDD_USER,
    password: process.env.BBDD_PASSWORD,
    server:process.env.BBDD_SERVER,
    port: Number(process.env.BBDD_PORT),
    database: process.env.BBDD_NAME,
    requestTimeout: 10 * 60 * 1000,     // 10 minutos (ajusta a gusto)
    connectionTimeout: 60 * 1000,       // 60s para conectar (opcional)
    options: {
      encrypt: true,
      trustServerCertificate: true
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

async function createConection() {
  try {
    const pool = await sql.connect(sqlConfig);
    console.log('✔️ Conexión a SQL Server establecida');
    return pool;
  } catch (err) {
    console.error('❌ Error conectando a SQL Server', err);
    throw err;
  }
}


module.exports = {
  createConection,
};