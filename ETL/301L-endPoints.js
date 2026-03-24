const sql      = require('mssql');
const connFunc = require('../db/conexion');  

// Inserts Gestion

async function insertBatchPresupuesto(objectConverted) {
 
  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`insertBatch llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  await conn.request().query('delete from dbo.gesPresupuestos');
  console.log('presupuestos borrados');

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Insertando batch ${i + 1}/${batches.length} (size: ${batch.length})`);

      const table = new sql.Table('gesPresupuestos', { schema: 'dbo', create: false });
      table.columns.add('intTemporadasId',          sql.Int,            { nullable: true });
      table.columns.add('fecha',                    sql.Date,           { nullable: false });
      table.columns.add('intCentrosCostosCodigo',   sql.VarChar(20),    { nullable: true  });
      table.columns.add('intLaboresCodigo',         sql.VarChar(20),    { nullable: true  });
      table.columns.add('intItemsFamiliasCodigo',   sql.VarChar(20),    { nullable: true  });
      table.columns.add('cantidad',                 sql.Decimal(18,6),  { nullable: false });
      table.columns.add('valorTotal',               sql.Decimal(18,6),  { nullable: false  });

      for (const rec of batch) {

        table.rows.add(
          rec.intTemporadasId,
          rec.fecha,
          rec.cCentroCosto,
          rec.cLabor,
          rec.cFamilia,
          rec.cantidad,
          rec.total
        );

      }

      await conn.request().bulk(table);

    }

      console.log( 'insertBatchPresupuestos completado');
  
};

async function insertBatchPresupuestoDistribuido(objectConverted,intTemporadasId) {

const records = Array.isArray(objectConverted) ? objectConverted : [];

console.log(`insertBatch llamado con ${records.length} registros`);
if (records.length === 0) {
  console.warn('No hay registros para insertar.');
  return;
}

const conn      = await connFunc.createConection();
const batchSize = 500;
const batches   = [];

await conn
  .request()
  .input('intTemporadasId', sql.Int, intTemporadasId)
  .query('delete from dbo.gesPresupuestosDistribuidos where intTemporadasId = @intTemporadasId');

  console.log('presupuesto Distribuido borrado');

for (let i = 0; i < records.length; i += batchSize) {
  batches.push(records.slice(i, i + batchSize));
}

for (let i = 0; i < batches.length; i++) {
  const batch = batches[i];
  console.log(`Insertando batch ${i + 1}/${batches.length} (size: ${batch.length})`);

    const table = new sql.Table('gesPresupuestosDistribuidos', { schema: 'dbo', create: false });
    table.columns.add('intTemporadasId',          sql.Int,            { nullable: true  });
    table.columns.add('fecha',                    sql.Date,           { nullable: false });
    table.columns.add('intCentrosCostosCodigo',   sql.VarChar(20),    { nullable: true  });
    table.columns.add('intLaboresCodigo',         sql.VarChar(20),    { nullable: true  });
    table.columns.add('intItemsFamiliasCodigo',   sql.VarChar(20),    { nullable: true  });
    table.columns.add('cantidad',                 sql.Decimal(18,6),  { nullable: false });
    table.columns.add('valorTotal',               sql.Decimal(18,6),  { nullable: false  });

    for (const rec of batch) {

      table.rows.add(
        rec.intTemporadasId,
        rec.fecha,
        rec.intCentrosCostosCodigo,
        rec.intLaboresCodigo,
        rec.intFamiliasCodigo,
        rec.cantidad,
        rec.valorTotal
      );

    }

    await conn.request().bulk(table);

  }

    //console.log( 'insertBatchStoreSales completado');

};

async function insertBatchGastos(objectConverted,fechaDesde) {
 
  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`insertBatch llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  await conn
    .request()
    .input('fechaDesde', sql.Date, fechaDesde)
    .query('delete from dbo.gesGastos where fecha >= @fechaDesde');
  
    console.log('gastos Sin Distribuir borrados');

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Insertando batch ${i + 1}/${batches.length} (size: ${batch.length})`);

      const table = new sql.Table('gesGastos', { schema: 'dbo', create: false });
      table.columns.add('distribucion',               sql.VarChar(50),    { nullable: false });
      table.columns.add('intEntidadesEmisoraCodigo',  sql.VarChar(20),    { nullable: false });
      table.columns.add('intDocumentosTipo',          sql.VarChar(10),    { nullable: false });
      table.columns.add('numeroDocumento',            sql.VarChar(50),    { nullable: false });
      table.columns.add('intEntidadesReceptoraCodigo',sql.VarChar(20),    { nullable: false });
      table.columns.add('intTemporadasId',            sql.Int,            { nullable: true  });
      table.columns.add('fecha',                      sql.Date,           { nullable: false });
      table.columns.add('intCentrosCostosCodigo',     sql.VarChar(20),    { nullable: true  });
      table.columns.add('intLaboresCodigo',           sql.VarChar(20),    { nullable: true  });
      table.columns.add('intItemsCodigo',             sql.VarChar(20),    { nullable: true  });
      table.columns.add('cantidad',                   sql.Decimal(18,6),  { nullable: false });
      table.columns.add('valorTotal',                 sql.Decimal(18,6),  { nullable: false });

      for (const rec of batch) {

        table.rows.add(
          rec.distribucion,
          rec.cEmisor,
          rec.tDocumento,
          rec.nDocumento,
          rec.cReceptor,
          rec.intTemporadasId,
          rec.fecha,
          rec.cCentroCosto,
          rec.cLabor,
          rec.cItem,
          rec.cantidad,
          rec.totalReal
        );

      }

      await conn.request().bulk(table);

    }

    console.log( 'insertBatchGastos completado');
  
};

async function insertBatchGastosDistribuidos(objectConverted,intTemporadasId) {
 
  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`insertBatch llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  await conn
    .request()
    .input('intTemporadasId', sql.Int, intTemporadasId)
    .query('delete from dbo.gesGastosDistribuidos where intTemporadasId = @intTemporadasId');
  
    console.log('gastos Distribuidos borrados');

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Insertando batch ${i + 1}/${batches.length} (size: ${batch.length})`);

      const table = new sql.Table('gesGastosDistribuidos', { schema: 'dbo', create: false });
      table.columns.add('intEntidadesEmisoraCodigo',      sql.VarChar(20),    { nullable: false });
      table.columns.add('intDocumentosTipo',              sql.VarChar(10),    { nullable: false });
      table.columns.add('numeroDocumento',                sql.VarChar(50),    { nullable: false });
      table.columns.add('intEntidadesReceptoraCodigo',    sql.VarChar(20),    { nullable: false });
      table.columns.add('intTemporadasId',                sql.Int,            { nullable: true  });
      table.columns.add('fecha',                          sql.Date,           { nullable: false });
      table.columns.add('intCentrosCostosCodigo',         sql.VarChar(20),    { nullable: true  });
      table.columns.add('intLaboresCodigo',               sql.VarChar(20),    { nullable: true  });
      table.columns.add('intItemsCodigo',                 sql.VarChar(20),    { nullable: true  });
      table.columns.add('cantidad',                       sql.Decimal(18,6),  { nullable: false });
      table.columns.add('valorTotal',                     sql.Decimal(18,6),  { nullable: false });

      for (const rec of batch) {

        table.rows.add(
          rec.intEntidadesEmisoraCodigo,
          rec.intDocumentosTipo,
          rec.numeroDocumento,
          rec.intEntidadesReceptoraCodigo,
          rec.intTemporadasId,
          rec.fecha,
          rec.intCentrosCostosCodigo,
          rec.intLaboresCodigo,
          rec.intItemsCodigo,
          rec.cantidad,
          rec.valorTotal
        );

      }

      await conn.request().bulk(table);

    }

      //console.log( 'insertBatchStoreSales completado');
  
};

async function insertBatchCostoMODirecta(objectConverted, fechaDesde) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`insertBatchCostoMODirecta llamado con ${records.length} registros`);

  if (records.length === 0) {
    console.warn('No hay registros para insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  // 🔹 Parse fechaDesde seguro (local)
  const m = fechaDesde.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) throw new Error("fechaDesde inválida. Usa YYYY-MM-DD");

  const anoDesde = Number(m[1]);
  const mesDesde = Number(m[2]);

  // 🔹 DELETE más simple
  await conn.request()
    .input('anoDesde', sql.Int, anoDesde)
    .input('mesDesde', sql.Int, mesDesde)
    .query(`
      delete from dbo.gesCostoManoObraDirecta
      where (ano > @anoDesde)
         or (ano = @anoDesde and mes >= @mesDesde)
    `);

  console.log(`Registros borrados desde ${anoDesde}-${String(mesDesde).padStart(2,'0')}`);

  // 🔹 dividir en batches
  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  for (let i = 0; i < batches.length; i++) {

    const batch = batches[i];
    console.log(`Insertando batch ${i + 1}/${batches.length} (size: ${batch.length})`);

    const table = new sql.Table('gesCostoManoObraDirecta', { schema: 'dbo', create: false });

    table.columns.add('intEmpresasCodigo',        sql.VarChar(20),   { nullable: false });
    table.columns.add('intSucursalesCodigo',      sql.VarChar(20),   { nullable: true  });
    table.columns.add('intCentrosCostosCodigo',   sql.VarChar(20),   { nullable: true  });
    table.columns.add('intLaboresCodigo',         sql.VarChar(20),   { nullable: true  });
    table.columns.add('intItemsCodigo',           sql.VarChar(20),   { nullable: true  });
    table.columns.add('remTrabajadoresCodigo',    sql.VarChar(20),   { nullable: false });
    table.columns.add('remContratosTipoCodigo',   sql.VarChar(20),   { nullable: true  });
    table.columns.add('remCargosCodigo',          sql.VarChar(20),   { nullable: true  });
    table.columns.add('remConceptosCodigo',       sql.VarChar(20),   { nullable: true  });
    table.columns.add('remBonificacionesTipo',    sql.VarChar(20),   { nullable: true  });
    table.columns.add('remBonificacionesCodigo',  sql.VarChar(20),   { nullable: true  });
    table.columns.add('intTeporadasId',           sql.Int,           { nullable: true  });
    table.columns.add('ano',                      sql.Int,           { nullable: false });
    table.columns.add('mes',                      sql.Int,           { nullable: false });
    table.columns.add('tipoRegistro',             sql.VarChar(20),   { nullable: true  });
    table.columns.add('jornadas',                 sql.Decimal(10,3), { nullable: true  });
    table.columns.add('rendimiento',              sql.Decimal(18,3), { nullable: true  });
    table.columns.add('horasExtras',              sql.Decimal(18,3), { nullable: true  });
    table.columns.add('valorAsignacion',          sql.Decimal(18,3), { nullable: true  });
    table.columns.add('ajusteLiquidacion',        sql.Decimal(18,3), { nullable: true  });
    table.columns.add('valorLiquidacion',         sql.Decimal(18,3), { nullable: true  });

    for (const rec of batch) {
      table.rows.add(
        rec.intEmpresasCodigo,
        rec.intSucursalesCodigo,
        rec.intCentrosCostosCodigo,
        rec.intLaboresCodigo,
        rec.intItemsCodigo,
        rec.remTrabajadoresCodigo,
        rec.remContratosTipoCodigo,
        rec.remCargosCodigo,
        rec.remConceptosCodigo,
        rec.remBonificacionesTipo,
        rec.remBonificacionesCodigo,
        rec.intTeporadasId,
        rec.ano,
        rec.mes,
        rec.tipoRegistro,
        rec.jornadas,
        rec.rendimiento,
        rec.horasExtras,
        rec.valorAsignacion,
        rec.ajusteLiquidacion,
        rec.valorLiquidacion
      );
    }

    await conn.request().bulk(table);
  }

  console.log('insertBatchCostoMODirecta completado');
};

async function insertBatchCostoMOcontratista(objectConverted, fechaDesde) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`insertBatchCostoMOcontratista llamado con ${records.length} registros`);

  if (records.length === 0) {
    console.warn('No hay registros para insertar.');
    return;
  }

  const conn = await connFunc.createConection();
  const batchSize = 500;
  const batches = [];

  // Parse fechaDesde seguro (local)
  const m = fechaDesde.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) throw new Error("fechaDesde inválida. Usa YYYY-MM-DD");

  const anoDesde = Number(m[1]);
  const mesDesde = Number(m[2]);

  // DELETE
  await conn.request()
    .input('anoDesde', sql.Int, anoDesde)
    .input('mesDesde', sql.Int, mesDesde)
    .query(`
      delete from dbo.gesCostoManoObraContratista
      where (ano > @anoDesde)
         or (ano = @anoDesde and mes >= @mesDesde)
    `);

  console.log(`Registros contratista borrados desde ${anoDesde}-${String(mesDesde).padStart(2, '0')}`);

  // dividir en batches
  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  for (let i = 0; i < batches.length; i++) {

    const batch = batches[i];
    console.log(`Insertando batch contratista ${i + 1}/${batches.length} (size: ${batch.length})`);

    const table = new sql.Table('gesCostoManoObraContratista', { schema: 'dbo', create: false });

    table.columns.add('intEmpresasCodigo',        sql.VarChar(20),   { nullable: false });
    table.columns.add('intSucursalesCodigo',      sql.VarChar(20),   { nullable: true  });
    table.columns.add('intCentrosCostosCodigo',   sql.VarChar(20),   { nullable: true  });
    table.columns.add('intLaboresCodigo',         sql.VarChar(20),   { nullable: true  });
    table.columns.add('intItemsCodigo',           sql.VarChar(20),   { nullable: true  });
    table.columns.add('remTrabajadoresCodigo',    sql.VarChar(20),   { nullable: false });
    table.columns.add('remContratosTipoCodigo',   sql.VarChar(20),   { nullable: true  });
    table.columns.add('remCargosCodigo',          sql.VarChar(20),   { nullable: true  });
    table.columns.add('remConceptosCodigo',       sql.VarChar(20),   { nullable: true  });
    table.columns.add('remBonificacionesTipo',    sql.VarChar(20),   { nullable: true  });
    table.columns.add('remBonificacionesCodigo',  sql.VarChar(20),   { nullable: true  });
    table.columns.add('intTeporadasId',           sql.Int,           { nullable: true  });
    table.columns.add('ano',                      sql.Int,           { nullable: false });
    table.columns.add('mes',                      sql.Int,           { nullable: false });
    table.columns.add('tipoRegistro',             sql.VarChar(20),   { nullable: true  });
    table.columns.add('jornadas',                 sql.Decimal(10,3), { nullable: true  });
    table.columns.add('rendimiento',              sql.Decimal(18,3), { nullable: true  });
    table.columns.add('horasExtras',              sql.Decimal(18,3), { nullable: true  });
    table.columns.add('valorAsignacion',          sql.Decimal(18,3), { nullable: true  });
    table.columns.add('ajusteLiquidacion',        sql.Decimal(18,3), { nullable: true  });
    table.columns.add('valorLiquidacion',         sql.Decimal(18,3), { nullable: true  });

    for (const rec of batch) {
      table.rows.add(
        rec.intEmpresasCodigo,
        rec.intSucursalesCodigo,
        rec.intCentrosCostosCodigo,
        rec.intLaboresCodigo,
        rec.intItemsCodigo,
        rec.remTrabajadoresCodigo,
        rec.remContratosTipoCodigo,
        rec.remCargosCodigo,
        rec.remConceptosCodigo,
        rec.remBonificacionesTipo,
        rec.remBonificacionesCodigo,
        rec.intTeporadasId,
        rec.ano,
        rec.mes,
        rec.tipoRegistro,
        rec.jornadas,
        rec.rendimiento,
        rec.horasExtras,
        rec.valorAsignacion,
        rec.ajusteLiquidacion,
        rec.valorLiquidacion
      );
    }

    await conn.request().bulk(table);
  }

  console.log('insertBatchCostoMOcontratista completado');
}

//Updates

//Estructura CC

async function updateBatchEmpresas(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`upsertBatchEmpresas (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único (evita colisiones entre ejecuciones concurrentes)
  const tmpTableName = `##TmpEmpresas_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intEmpresasCodigo VARCHAR(20)  NOT NULL,
      intEmpresasNombre VARCHAR(100) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intEmpresasCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intEmpresasNombre', sql.VarChar(100), { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cEmpresa,
          rec.nEmpresa
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE existentes + INSERT nuevos
    //    Ojo: deduplicamos src por codigo para evitar "The MERGE statement attempted to UPDATE or DELETE the same row more than once"
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intEmpresasCodigo,
          MAX(intEmpresasNombre) AS intEmpresasNombre
        FROM ${tmpTableName}
        GROUP BY intEmpresasCodigo
      )
      MERGE dbo.intEmpresas AS tgt
      USING src
        ON tgt.intEmpresasCodigo = src.intEmpresasCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intEmpresasNombre = src.intEmpresasNombre

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intEmpresasCodigo, intEmpresasNombre)
        VALUES (src.intEmpresasCodigo, src.intEmpresasNombre);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updatetBatchEmpresas (MERGE) completado OK');
  } catch (err) {
    // Si falla el MERGE, intenta limpiar la temporal (sin romper el error original)
    try { await conn.request().query(`IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL DROP TABLE ${tmpTableName};`); } catch (_) {}
    throw err;
  }

};

async function updateBatchSucursales(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchSucursales (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpSucursales_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intSucursalesCodigo  VARCHAR(20)  NOT NULL,
      intSucursalesNombre  VARCHAR(100)  NULL,
      intEmpresasCodigo     VARCHAR(20) NOT NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intSucursalesCodigo', sql.VarChar(20),   { nullable: false });
      table.columns.add('intSucursalesNombre', sql.VarChar(100),  { nullable: true  });
      table.columns.add('intEmpresasCodigo',   sql.VarChar(20),   { nullable: false });

      for (const rec of batch) {
        table.rows.add(
          rec.cSucursal,
          rec.nSucursal,
          rec.cEmpresa
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE existentes + INSERT nuevos
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intSucursalesCodigo,
          MAX(intSucursalesNombre) AS intSucursalesNombre,
          MAX(intEmpresasCodigo)    AS intEmpresasCodigo
        FROM ${tmpTableName}
        GROUP BY intSucursalesCodigo
      )
      MERGE dbo.intSucursales AS tgt
      USING src
        ON tgt.intSucursalesCodigo = src.intSucursalesCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intSucursalesNombre = src.intSucursalesNombre,
          tgt.intEmpresasCodigo    = src.intEmpresasCodigo

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (
          intSucursalesCodigo,
          intSucursalesNombre,
          intEmpresasCodigo
        )
        VALUES (
          src.intSucursalesCodigo,
          src.intSucursalesNombre,
          src.intEmpresasCodigo
        );

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchSucursales (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchPredios(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchPredios (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpPredios_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intPrediosCodigo VARCHAR(20)  NOT NULL,
      intPrediosNombre VARCHAR(100) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intPrediosCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intPrediosNombre', sql.VarChar(100), { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cPredio,
          rec.nPredio
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE existentes + INSERT nuevos
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intPrediosCodigo,
          MAX(intPrediosNombre) AS intPrediosNombre
        FROM ${tmpTableName}
        GROUP BY intPrediosCodigo
      )
      MERGE dbo.intPredios AS tgt
      USING src
        ON tgt.intPrediosCodigo = src.intPrediosCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intPrediosNombre = src.intPrediosNombre

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intPrediosCodigo, intPrediosNombre)
        VALUES (src.intPrediosCodigo, src.intPrediosNombre);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchPredios (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchCuarteles(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchCuarteles (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpCuarteles_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intCuartelesCodigo VARCHAR(20)  NOT NULL,
      intCuartelesNombre VARCHAR(100) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intCuartelesCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intCuartelesNombre', sql.VarChar(100), { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cCuartel,
          rec.nCuartel
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE existentes + INSERT nuevos
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intCuartelesCodigo,
          MAX(intCuartelesNombre) AS intCuartelesNombre
        FROM ${tmpTableName}
        GROUP BY intCuartelesCodigo
      )
      MERGE dbo.intCuarteles AS tgt
      USING src
        ON tgt.intCuartelesCodigo = src.intCuartelesCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intCuartelesNombre = src.intCuartelesNombre

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intCuartelesCodigo, intCuartelesNombre)
        VALUES (src.intCuartelesCodigo, src.intCuartelesNombre);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchCuarteles (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchCentrosCosto(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchCentrosCosto (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpCentrosCosto_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intCentrosCostosCodigo  VARCHAR(20)  NOT NULL,
      intCentrosCostosNombre  VARCHAR(100) NULL,
      intCentrosCostosTipoId  INT          NULL,
      intEmpresasCodigo       VARCHAR(20)  NOT NULL,
      intSucursalesCodigo     VARCHAR(20)  NULL,
      intPrediosCodigo        VARCHAR(20)  NULL,
      intCuartelesCodigo      VARCHAR(20)  NULL,
      intVariedadesCodigo     VARCHAR(20)  NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intCentrosCostosCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intCentrosCostosNombre', sql.VarChar(100), { nullable: true  });
      table.columns.add('intCentrosCostosTipoId', sql.Int,          { nullable: true  });
      table.columns.add('intEmpresasCodigo',      sql.VarChar(20),  { nullable: false });
      table.columns.add('intSucursalesCodigo',    sql.VarChar(20),  { nullable: true  });
      table.columns.add('intPrediosCodigo',       sql.VarChar(20),  { nullable: true  });
      table.columns.add('intCuartelesCodigo',     sql.VarChar(20),  { nullable: true  });
      table.columns.add('intVariedadesCodigo',    sql.VarChar(20),  { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cCentroCosto,
          rec.nCentroCosto,
          rec.tCentroCosto,
          rec.cEmpresa,
          rec.cSucursal,
          rec.cPredio,
          rec.cCuartel,
          rec.cVariedad
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE existentes + INSERT nuevos
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intCentrosCostosCodigo,
          MAX(intCentrosCostosNombre) AS intCentrosCostosNombre,
          MAX(intCentrosCostosTipoId) AS intCentrosCostosTipoId,
          MAX(intEmpresasCodigo)      AS intEmpresasCodigo,
          MAX(intSucursalesCodigo)    AS intSucursalesCodigo,
          MAX(intPrediosCodigo)       AS intPrediosCodigo,
          MAX(intCuartelesCodigo)     AS intCuartelesCodigo,
          MAX(intVariedadesCodigo)    AS intVariedadesCodigo
        FROM ${tmpTableName}
        GROUP BY intCentrosCostosCodigo
      )
      MERGE dbo.intCentrosCostos AS tgt
      USING src
        ON tgt.intCentrosCostosCodigo = src.intCentrosCostosCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intCentrosCostosNombre  = src.intCentrosCostosNombre,
          tgt.intCentrosCostosTipoId  = src.intCentrosCostosTipoId,
          tgt.intEmpresasCodigo       = src.intEmpresasCodigo,
          tgt.intSucursalesCodigo     = src.intSucursalesCodigo,
          tgt.intPrediosCodigo        = src.intPrediosCodigo,
          tgt.intCuartelesCodigo      = src.intCuartelesCodigo,
          tgt.intVariedadesCodigo     = src.intVariedadesCodigo

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (
          intCentrosCostosCodigo,
          intCentrosCostosNombre,
          intCentrosCostosTipoId,
          intEmpresasCodigo,
          intSucursalesCodigo,
          intPrediosCodigo,
          intCuartelesCodigo,
          intVariedadesCodigo
        )
        VALUES (
          src.intCentrosCostosCodigo,
          src.intCentrosCostosNombre,
          src.intCentrosCostosTipoId,
          src.intEmpresasCodigo,
          src.intSucursalesCodigo,
          src.intPrediosCodigo,
          src.intCuartelesCodigo,
          src.intVariedadesCodigo
        );

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchCentrosCosto (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchCentrosCostoDistribucion(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchCentrosCostoDistribucion (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL
  const tmpTableName = `##TmpCentrosCostosDistribucion_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intCentrosCostosCodigoOrigen  VARCHAR(20) NULL,
      intCentrosCostosCodigoDestino VARCHAR(20) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intCentrosCostosCodigoOrigen',  sql.VarChar(20), { nullable: true });
      table.columns.add('intCentrosCostosCodigoDestino', sql.VarChar(20), { nullable: true });

      for (const rec of batch) {
        table.rows.add(
          rec.cOrigen,
          rec.cDestino
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intCentrosCostosCodigoOrigen,
          intCentrosCostosCodigoDestino
        FROM ${tmpTableName}
        GROUP BY
          intCentrosCostosCodigoOrigen,
          intCentrosCostosCodigoDestino
      )
      MERGE dbo.intCentrosCostosDistribucion AS tgt
      USING src
        ON  ISNULL(tgt.intCentrosCostosCodigoOrigen, '')  = ISNULL(src.intCentrosCostosCodigoOrigen, '')
        AND ISNULL(tgt.intCentrosCostosCodigoDestino, '') = ISNULL(src.intCentrosCostosCodigoDestino, '')

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intCentrosCostosCodigoOrigen  = src.intCentrosCostosCodigoOrigen,
          tgt.intCentrosCostosCodigoDestino = src.intCentrosCostosCodigoDestino

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intCentrosCostosCodigoOrigen, intCentrosCostosCodigoDestino)
        VALUES (src.intCentrosCostosCodigoOrigen, src.intCentrosCostosCodigoDestino);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchCentrosCostoDistribucion (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

//Estructura Especies

async function updateBatchEspecies(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchEspecies (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpEspecies_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intEspeciesCodigo VARCHAR(20)  NOT NULL,
      intEspeciesNombre VARCHAR(100) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intEspeciesCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intEspeciesNombre', sql.VarChar(100), { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cEspecie,
          rec.nEspecie
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intEspeciesCodigo,
          MAX(intEspeciesNombre) AS intEspeciesNombre
        FROM ${tmpTableName}
        GROUP BY intEspeciesCodigo
      )
      MERGE dbo.intEspecies AS tgt
      USING src
        ON tgt.intEspeciesCodigo = src.intEspeciesCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intEspeciesNombre = src.intEspeciesNombre

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intEspeciesCodigo, intEspeciesNombre)
        VALUES (src.intEspeciesCodigo, src.intEspeciesNombre);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchEspecies (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchVariedades(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchVariedades (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpVariedades_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intVariedadesCodigo VARCHAR(20)  NOT NULL,
      intVariedadesNombre VARCHAR(100) NULL,
      intEspeciesCodigo   VARCHAR(20)  NOT NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intVariedadesCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intVariedadesNombre', sql.VarChar(100), { nullable: true  });
      table.columns.add('intEspeciesCodigo',   sql.VarChar(20),  { nullable: false });

      for (const rec of batch) {
        table.rows.add(
          rec.cVariedad,
          rec.nVariedad,
          rec.cEspecie
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intVariedadesCodigo,
          MAX(intVariedadesNombre) AS intVariedadesNombre,
          MAX(intEspeciesCodigo)   AS intEspeciesCodigo
        FROM ${tmpTableName}
        GROUP BY intVariedadesCodigo
      )
      MERGE dbo.intVariedades AS tgt
      USING src
        ON tgt.intVariedadesCodigo = src.intVariedadesCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intVariedadesNombre = src.intVariedadesNombre,
          tgt.intEspeciesCodigo   = src.intEspeciesCodigo

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intVariedadesCodigo, intVariedadesNombre, intEspeciesCodigo)
        VALUES (src.intVariedadesCodigo, src.intVariedadesNombre, src.intEspeciesCodigo);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchVariedades (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

//Estructura Faenas

async function updateBatchFaenas(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchFaenas (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpFaenas_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intFaenasCodigo VARCHAR(20)  NOT NULL,
      intFaenasNombre VARCHAR(100) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intFaenasCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intFaenasNombre', sql.VarChar(100), { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cFaena,
          rec.nFaena
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intFaenasCodigo,
          MAX(intFaenasNombre) AS intFaenasNombre
        FROM ${tmpTableName}
        GROUP BY intFaenasCodigo
      )
      MERGE dbo.intFaenas AS tgt
      USING src
        ON tgt.intFaenasCodigo = src.intFaenasCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intFaenasNombre = src.intFaenasNombre

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intFaenasCodigo, intFaenasNombre)
        VALUES (src.intFaenasCodigo, src.intFaenasNombre);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchFaenas (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchLabores(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchLabores (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpLabores_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intLaboresCodigo VARCHAR(20)  NOT NULL,
      intLaboresNombre VARCHAR(100) NULL,
      intFaenasCodigo  VARCHAR(20)  NOT NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intLaboresCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intLaboresNombre', sql.VarChar(100), { nullable: true  });
      table.columns.add('intFaenasCodigo',  sql.VarChar(20),  { nullable: false });

      for (const rec of batch) {
        table.rows.add(
          rec.cLabor,
          rec.nLabor,
          rec.cFaena
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intLaboresCodigo,
          MAX(intLaboresNombre) AS intLaboresNombre,
          MAX(intFaenasCodigo)  AS intFaenasCodigo
        FROM ${tmpTableName}
        GROUP BY intLaboresCodigo
      )
      MERGE dbo.intLabores AS tgt
      USING src
        ON tgt.intLaboresCodigo = src.intLaboresCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intLaboresNombre = src.intLaboresNombre,
          tgt.intFaenasCodigo  = src.intFaenasCodigo

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intLaboresCodigo, intLaboresNombre, intFaenasCodigo)
        VALUES (src.intLaboresCodigo, src.intLaboresNombre, src.intFaenasCodigo);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchLabores (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

//Estrcutura Items  

async function updateBatchItemsSuperFamilias(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchItemsSuperFamilias (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpItemsSuperFamilias_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intItemsSuperFamiliasCodigo VARCHAR(20)  NOT NULL,
      intItemsSuperFamiliasNombre VARCHAR(100) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intItemsSuperFamiliasCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intItemsSuperFamiliasNombre', sql.VarChar(100), { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cSuperFamilia,
          rec.nSuperFamilia
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intItemsSuperFamiliasCodigo,
          MAX(intItemsSuperFamiliasNombre) AS intItemsSuperFamiliasNombre
        FROM ${tmpTableName}
        GROUP BY intItemsSuperFamiliasCodigo
      )
      MERGE dbo.intItemsSuperFamilias AS tgt
      USING src
        ON tgt.intItemsSuperFamiliasCodigo = src.intItemsSuperFamiliasCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intItemsSuperFamiliasNombre = src.intItemsSuperFamiliasNombre

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intItemsSuperFamiliasCodigo, intItemsSuperFamiliasNombre)
        VALUES (src.intItemsSuperFamiliasCodigo, src.intItemsSuperFamiliasNombre);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchItemsSuperFamilias (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchItemsFamilias(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchItemsFamilias (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpItemsFamilias_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intItemsFamiliasCodigo      VARCHAR(20)  NOT NULL,
      intItemsFamiliasNombre      VARCHAR(100) NULL,
      intItemsSuperFamiliasCodigo VARCHAR(20)  NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intItemsFamiliasCodigo',      sql.VarChar(20),  { nullable: false });
      table.columns.add('intItemsFamiliasNombre',      sql.VarChar(100), { nullable: true  });
      table.columns.add('intItemsSuperFamiliasCodigo', sql.VarChar(20),  { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cFamilia,
          rec.nFamilia,
          rec.cSuperFamilia
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intItemsFamiliasCodigo,
          MAX(intItemsFamiliasNombre)      AS intItemsFamiliasNombre,
          MAX(intItemsSuperFamiliasCodigo) AS intItemsSuperFamiliasCodigo
        FROM ${tmpTableName}
        GROUP BY intItemsFamiliasCodigo
      )
      MERGE dbo.intItemsFamilias AS tgt
      USING src
        ON tgt.intItemsFamiliasCodigo = src.intItemsFamiliasCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intItemsFamiliasNombre      = src.intItemsFamiliasNombre,
          tgt.intItemsSuperFamiliasCodigo = src.intItemsSuperFamiliasCodigo

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intItemsFamiliasCodigo, intItemsFamiliasNombre, intItemsSuperFamiliasCodigo)
        VALUES (src.intItemsFamiliasCodigo, src.intItemsFamiliasNombre, src.intItemsSuperFamiliasCodigo);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchItemsFamilias (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchItemsSubFamilias(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchSubFamilias (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpItemsSubFamilias_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intItemsSubFamiliasCodigo VARCHAR(20)  NOT NULL,
      intItemsSubFamiliasNombre VARCHAR(100) NULL,
      intItemsFamiliasCodigo    VARCHAR(20)  NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intItemsSubFamiliasCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intItemsSubFamiliasNombre', sql.VarChar(100), { nullable: true  });
      table.columns.add('intItemsFamiliasCodigo',    sql.VarChar(20),  { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cSubFamilia,
          rec.nSubFamilia,
          rec.cFamilia
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intItemsSubFamiliasCodigo,
          MAX(intItemsSubFamiliasNombre) AS intItemsSubFamiliasNombre,
          MAX(intItemsFamiliasCodigo)    AS intItemsFamiliasCodigo
        FROM ${tmpTableName}
        GROUP BY intItemsSubFamiliasCodigo
      )
      MERGE dbo.intItemsSubFamilias AS tgt
      USING src
        ON tgt.intItemsSubFamiliasCodigo = src.intItemsSubFamiliasCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intItemsSubFamiliasNombre = src.intItemsSubFamiliasNombre,
          tgt.intItemsFamiliasCodigo    = src.intItemsFamiliasCodigo

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intItemsSubFamiliasCodigo, intItemsSubFamiliasNombre, intItemsFamiliasCodigo)
        VALUES (src.intItemsSubFamiliasCodigo, src.intItemsSubFamiliasNombre, src.intItemsFamiliasCodigo);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchSubFamilias (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchItems(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchItems (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpItems_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intItemsCodigo            VARCHAR(20)  NOT NULL,
      intItemsNombre            VARCHAR(100) NULL,
      intItemsSubFamiliasCodigo VARCHAR(20)  NULL,
      intUnidadesCodigo         VARCHAR(20)  NULL,
      intItemsArea              VARCHAR(20)  NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intItemsCodigo',            sql.VarChar(20),  { nullable: false });
      table.columns.add('intItemsNombre',            sql.VarChar(100), { nullable: true  });
      table.columns.add('intItemsSubFamiliasCodigo', sql.VarChar(20),  { nullable: true  });
      table.columns.add('intUnidadesCodigo',         sql.VarChar(20),  { nullable: true  });
      table.columns.add('intItemsArea',             sql.VarChar(20),  { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cItem,
          rec.nItem,
          rec.cSubFamilia,
          rec.cUnidad,
          rec.areaItem
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intItemsCodigo,
          MAX(intItemsNombre)            AS intItemsNombre,
          MAX(intItemsSubFamiliasCodigo) AS intItemsSubFamiliasCodigo,
          MAX(intUnidadesCodigo)         AS intUnidadesCodigo,
          MAX(intItemsArea)             AS intItemsArea
        FROM ${tmpTableName}
        GROUP BY intItemsCodigo
      )
      MERGE dbo.intItems AS tgt
      USING src
        ON tgt.intItemsCodigo = src.intItemsCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intItemsNombre            = src.intItemsNombre,
          tgt.intItemsSubFamiliasCodigo = src.intItemsSubFamiliasCodigo,
          tgt.intUnidadesCodigo         = src.intUnidadesCodigo,
          tgt.intItemsArea              = src.intItemsArea

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (
          intItemsCodigo,
          intItemsNombre,
          intItemsSubFamiliasCodigo,
          intUnidadesCodigo,
          intItemsArea
        )
        VALUES (
          src.intItemsCodigo,
          src.intItemsNombre,
          src.intItemsSubFamiliasCodigo,
          src.intUnidadesCodigo,
          src.intItemsArea
        );

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchItems (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

async function updateBatchUnidades(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchUnidades (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // Tabla temporal GLOBAL con nombre único
  const tmpTableName = `##TmpUnidades_${Date.now()}`;

  // 1) Crear tabla temporal
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      intUnidadesCodigo VARCHAR(20)  NOT NULL,
      intUnidadesNombre VARCHAR(100) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  try {
    // 2) BULK LOAD a la temporal
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

      const table = new sql.Table(tmpTableName);
      table.create = false;

      table.columns.add('intUnidadesCodigo', sql.VarChar(20),  { nullable: false });
      table.columns.add('intUnidadesNombre', sql.VarChar(100), { nullable: true  });

      for (const rec of batch) {
        table.rows.add(
          rec.cUnidad,
          rec.nUnidad
        );
      }

      await conn.request().bulk(table);
    }

    // 3) MERGE: UPDATE + INSERT (dedupe por codigo)
    const mergeSql = `
      ;WITH src AS (
        SELECT
          intUnidadesCodigo,
          MAX(intUnidadesNombre) AS intUnidadesNombre
        FROM ${tmpTableName}
        GROUP BY intUnidadesCodigo
      )
      MERGE dbo.intUnidades AS tgt
      USING src
        ON tgt.intUnidadesCodigo = src.intUnidadesCodigo

      WHEN MATCHED THEN
        UPDATE SET
          tgt.intUnidadesNombre = src.intUnidadesNombre

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (intUnidadesCodigo, intUnidadesNombre)
        VALUES (src.intUnidadesCodigo, src.intUnidadesNombre);

      DROP TABLE ${tmpTableName};
    `;

    await conn.request().query(mergeSql);

    console.log('updateBatchUnidades (MERGE) completado OK');
  } catch (err) {
    // Limpieza best-effort
    try {
      await conn.request().query(`
        IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
          DROP TABLE ${tmpTableName};
      `);
    } catch (_) {}
    throw err;
  }
};

//Mano de Obra

async function updateBatchTrabajadores(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchTrabajadores (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // *** Tabla temporal global ***
  const tmpTableName = '##TmpTrabajadores';

  // 1) Crear tabla temporal GLOBAL
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      remTrabajadoresCodigo VARCHAR(20)   NOT NULL,
      remTrabajadoresNombre VARCHAR(100)  NULL
    );
  `;

  await conn.request().query(createTmpSql);

  // 2) BULK LOAD en la tabla temporal global
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

    const table = new sql.Table(tmpTableName);
    table.create = false;

    table.columns.add('remTrabajadoresCodigo', sql.VarChar(20),  { nullable: false });
    table.columns.add('remTrabajadoresNombre', sql.VarChar(100), { nullable: true  });

    for (const rec of batch) {
      table.rows.add(
        rec.cTrabajador,  // → remTrabajadoresCodigo
        rec.nTrabajador   // → remTrabajadoresNombre
      );
    }

    await conn.request().bulk(table);
  }

  // 3) MERGE: UPDATE + INSERT
  const mergeSql = `
    MERGE dbo.remTrabajadores AS tgt
    USING ${tmpTableName} AS src
      ON tgt.remTrabajadoresCodigo = src.remTrabajadoresCodigo

    WHEN MATCHED THEN
      UPDATE SET
        tgt.remTrabajadoresNombre = src.remTrabajadoresNombre

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        remTrabajadoresCodigo,
        remTrabajadoresNombre
      )
      VALUES (
        src.remTrabajadoresCodigo,
        src.remTrabajadoresNombre
      );

    DROP TABLE ${tmpTableName};
  `;

  await conn.request().query(mergeSql);

  console.log('updateBatchTrabajadores (MERGE) completado OK');
};

async function updateBatchContratosTipo(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchContratosTipo (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // *** Tabla temporal global ***
  const tmpTableName = '##TmpContratosTipo';

  // 1) Crear tabla temporal GLOBAL
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      remContratosTiposCodigo VARCHAR(20)  NOT NULL,
      remContratosTiposNombre VARCHAR(50)  NULL
    );
  `;

  await conn.request().query(createTmpSql);

  // 2) BULK LOAD en la tabla temporal global
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

    const table = new sql.Table(tmpTableName);
    table.create = false;

    table.columns.add('remContratosTiposCodigo', sql.VarChar(20), { nullable: false });
    table.columns.add('remContratosTiposNombre', sql.VarChar(50), { nullable: true  });

    for (const rec of batch) {
      table.rows.add(
        (rec.cTipoContrato ?? '').trim(),  // → remContratosTiposCodigo
        rec.nTipoContrato                  // → remContratosTiposNombre
      );
    }

    await conn.request().bulk(table);
  }

  // 3) MERGE: UPDATE + INSERT
  const mergeSql = `
    MERGE dbo.remContratosTipo AS tgt
    USING ${tmpTableName} AS src
      ON tgt.remContratosTiposCodigo = src.remContratosTiposCodigo

    WHEN MATCHED THEN
      UPDATE SET
        tgt.remContratosTiposNombre = src.remContratosTiposNombre

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        remContratosTiposCodigo,
        remContratosTiposNombre
      )
      VALUES (
        src.remContratosTiposCodigo,
        src.remContratosTiposNombre
      );

    DROP TABLE ${tmpTableName};
  `;

  await conn.request().query(mergeSql);

  console.log('updateBatchContratosTipo (MERGE) completado OK');
};

async function updateBatchCargos(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchCargos (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // *** Tabla temporal global ***
  const tmpTableName = '##TmpCargos';

  // 1) Crear tabla temporal GLOBAL
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      remCargosCodigo VARCHAR(20) NOT NULL,
      remCargosNombre VARCHAR(50) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  // 2) BULK LOAD en la tabla temporal global
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

    const table = new sql.Table(tmpTableName);
    table.create = false;

    table.columns.add('remCargosCodigo', sql.VarChar(20), { nullable: false });
    table.columns.add('remCargosNombre', sql.VarChar(50), { nullable: true  });

    for (const rec of batch) {
      table.rows.add(
        (rec.cCargo ?? '').trim(), // → remCargosCodigo
        rec.nCargo                 // → remCargosNombre
      );
    }

    await conn.request().bulk(table);
  }

  // 3) MERGE: UPDATE + INSERT
  const mergeSql = `
    MERGE dbo.remCargos AS tgt
    USING ${tmpTableName} AS src
      ON tgt.remCargosCodigo = src.remCargosCodigo

    WHEN MATCHED THEN
      UPDATE SET
        tgt.remCargosNombre = src.remCargosNombre

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        remCargosCodigo,
        remCargosNombre
      )
      VALUES (
        src.remCargosCodigo,
        src.remCargosNombre
      );

    DROP TABLE ${tmpTableName};
  `;

  await conn.request().query(mergeSql);

  console.log('updateBatchCargos (MERGE) completado OK');
};

async function updateBatchConceptos(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updataBatchConceptos (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // *** Tabla temporal global ***
  const tmpTableName = '##TmpConceptos';

  // 1) Crear tabla temporal GLOBAL
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      remConceptosCodigo VARCHAR(20) NOT NULL,
      remConceptosNombre VARCHAR(50) NULL
    );
  `;

  await conn.request().query(createTmpSql);

  // 2) BULK LOAD en la tabla temporal global
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

    const table = new sql.Table(tmpTableName);
    table.create = false;

    table.columns.add('remConceptosCodigo', sql.VarChar(20), { nullable: false });
    table.columns.add('remConceptosNombre', sql.VarChar(50), { nullable: true  });

    for (const rec of batch) {
      table.rows.add(
        (rec.cConcepto ?? '').trim(), // → remConceptosCodigo
        rec.nConcepto                 // → remConceptosNombre
      );
    }

    await conn.request().bulk(table);
  }

  // 3) MERGE: UPDATE + INSERT
  const mergeSql = `
    MERGE dbo.remConceptos AS tgt
    USING ${tmpTableName} AS src
      ON tgt.remConceptosCodigo = src.remConceptosCodigo

    WHEN MATCHED THEN
      UPDATE SET
        tgt.remConceptosNombre = src.remConceptosNombre

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        remConceptosCodigo,
        remConceptosNombre
      )
      VALUES (
        src.remConceptosCodigo,
        src.remConceptosNombre
      );

    DROP TABLE ${tmpTableName};
  `;

  await conn.request().query(mergeSql);

  console.log('updataBatchConceptos (MERGE) completado OK');
};

async function updateBatchBonificaciones(objectConverted) {

  const records = Array.isArray(objectConverted) ? objectConverted : [];

  console.log(`updateBatchBonificaciones (MERGE) llamado con ${records.length} registros`);
  if (records.length === 0) {
    console.warn('No hay registros para actualizar/insertar.');
    return;
  }

  const conn      = await connFunc.createConection();
  const batchSize = 500;
  const batches   = [];

  for (let i = 0; i < records.length; i += batchSize) {
    batches.push(records.slice(i, i + batchSize));
  }

  // *** Tabla temporal global ***
  const tmpTableName = '##TmpBonificaciones';

  // 1) Crear tabla temporal GLOBAL
  const createTmpSql = `
    IF OBJECT_ID('tempdb..${tmpTableName}') IS NOT NULL
      DROP TABLE ${tmpTableName};

    CREATE TABLE ${tmpTableName} (
      remBonificacionesCodigo  VARCHAR(20)  NOT NULL,
      remBonificacionesNombre VARCHAR(100)  NULL,
      remBonificacionesTipo   VARCHAR(20)   NOT NULL
    );
  `;

  await conn.request().query(createTmpSql);

  // 2) BULK LOAD en la tabla temporal global
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Cargando batch ${i + 1}/${batches.length} en tabla temporal (size: ${batch.length})`);

    const table = new sql.Table(tmpTableName);
    table.create = false;

    table.columns.add('remBonificacionesCodigo', sql.VarChar(20),  { nullable: false });
    table.columns.add('remBonificacionesNombre', sql.VarChar(100), { nullable: true  });
    table.columns.add('remBonificacionesTipo',   sql.VarChar(20),  { nullable: false  });

    for (const rec of batch) {
      table.rows.add(
        (rec.cBonificacion ?? '').trim(), // → remBonificacionesCodigo
        rec.nBonificacion,                // → remBonificacionesNombre
        (rec.tipo ?? '').trim()           // → remBonificacionesTipo
      );
    }

    await conn.request().bulk(table);
  }

  // 3) MERGE: UPDATE + INSERT
  const mergeSql = `
    MERGE dbo.remBonificaciones AS tgt
    USING ${tmpTableName} AS src
      ON tgt.remBonificacionesCodigo = src.remBonificacionesCodigo

    WHEN MATCHED THEN
      UPDATE SET
        tgt.remBonificacionesNombre = src.remBonificacionesNombre,
        tgt.remBonificacionesTipo   = src.remBonificacionesTipo

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        remBonificacionesCodigo,
        remBonificacionesNombre,
        remBonificacionesTipo
      )
      VALUES (
        src.remBonificacionesCodigo,
        src.remBonificacionesNombre,
        src.remBonificacionesTipo
      );

    DROP TABLE ${tmpTableName};
  `;

  await conn.request().query(mergeSql);

  console.log('updateBatchBonificaciones (MERGE) completado OK');
}

module.exports = { 

  insertBatchPresupuesto,
  insertBatchPresupuestoDistribuido,
  insertBatchGastos,
  insertBatchGastosDistribuidos,
  insertBatchCostoMODirecta,
  insertBatchCostoMOcontratista,

  updateBatchEmpresas,
  updateBatchSucursales,
  updateBatchPredios,
  updateBatchCuarteles,
  updateBatchCentrosCosto,
  updateBatchCentrosCostoDistribucion,

  updateBatchEspecies,
  updateBatchVariedades,

  updateBatchFaenas,
  updateBatchLabores,

  updateBatchItemsSuperFamilias,
  updateBatchItemsFamilias,
  updateBatchItemsSubFamilias,
  updateBatchItems,
  updateBatchUnidades,

  updateBatchTrabajadores,
  updateBatchContratosTipo,
  updateBatchCargos,
  updateBatchConceptos,
  updateBatchBonificaciones,


}