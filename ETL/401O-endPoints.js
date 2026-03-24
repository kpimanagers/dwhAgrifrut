const connFunc = require('../db/conexion');
const sql = require('mssql');

//Helpers

async function getTemporadas(){

    const conn  = await connFunc.createConection();

    var query = `select * from intTemporadas;`;

    const res = await conn.request().query(query);

    return res.recordset;

};

async function getDistributionConfig(intTemporadasId) {

  const conn = await connFunc.createConection();

  const res = await conn
    .request()
    .input('intTemporadasId', sql.Int, intTemporadasId)
    .query(`
      select
        temcd.intTemporadasId,
        ccd.intCentrosCostosCodigoOrigen,
        ccd.intCentrosCostosCodigoDestino,
        cast(temcd.hectareas as decimal(18,6))
          / nullif(
              SUM(temcd.hectareas) over (
                partition by temcd.intTemporadasId, ccd.intCentrosCostosCodigoOrigen
              ),
              0
            ) as factor
      from intCentrosCostosDistribucion as ccd
      left join intTemporadasConfiguracion as temcd
        on ccd.intCentrosCostosCodigoDestino = temcd.intCentrosCostosCodigo
      left join intTemporadasConfiguracion as temco
        on ccd.intCentrosCostosCodigoOrigen = temco.intCentrosCostosCodigo
      where temcd.intTemporadasId = @intTemporadasId
      order by
        temcd.intTemporadasId,
        ccd.intCentrosCostosCodigoOrigen,
        ccd.intCentrosCostosCodigoDestino;
    `);

  const rows = res.recordset || [];

  // Agrupar por intCentrosCostosCodigoOrigen
  const grouped = Object.values(
    rows.reduce((acc, row) => {

      const key = String(row.intCentrosCostosCodigoOrigen ?? '').trim();

      if (!acc[key]) {
        acc[key] = {
          intTemporadasId: row.intTemporadasId,
          intCentrosCostosCodigoOrigen: key,
          destinos: []
        };
      }

      acc[key].destinos.push({
        intCentrosCostosCodigoDestino: String(row.intCentrosCostosCodigoDestino ?? '').trim(),
        factor: Number(row.factor ?? 0)
      });

      return acc;
    }, {})
  );

  return grouped;

};


//Extract functions

//Transform functions

async function distribuirGastos(intTemporadasId) {

  const factoresTemporada = await getDistributionConfig(intTemporadasId);
  const conn = await connFunc.createConection();

  const gastosTemporadaRes = await conn
    .request()
    .input('intTemporadasId', sql.Int, intTemporadasId)
    .query(`
      select * from gesGastos where intTemporadasId = @intTemporadasId;`);

  const gastosTemporada = gastosTemporadaRes.recordset || [];

  // Mapa: intCentrosCostosCodigoOrigen -> destinos[]
  const distribucionMap = new Map(
    (factoresTemporada ?? []).map(f => [
      f.intCentrosCostosCodigoOrigen,
      f.destinos || []
    ])
  );

  const gastosDistribuidos = gastosTemporada.flatMap(gasto => {

    const origen = String(gasto.intCentrosCostosCodigo ?? '').trim();
    const destinos = distribucionMap.get(origen);

    const baseValorTotal = Number(gasto.valorTotal ?? 0);
    const baseCantidad   = Number(gasto.cantidad ?? 0);

    // Sin distribución → se deja tal cual (normalizamos bases)
    if (!destinos || destinos.length === 0) {
      return [{
        ...gasto,
        intCentrosCostosCodigo: origen,
        valorTotal: baseValorTotal,
        cantidad: baseCantidad
      }];
    }

    // Con distribución → se abre por cada destino
    return destinos.map(d => {
      const destino = String(d.intCentrosCostosCodigoDestino ?? '').trim();
      const factor  = Number(d.factor ?? 0);

      return {
        ...gasto,
        intCentrosCostosCodigo: destino,
        valorTotal: baseValorTotal * factor,
        cantidad: baseCantidad * factor
      };
    });
  });

  return gastosDistribuidos;

}

async function distribuirPresupuesto(intTemporadasId) {

  const factoresTemporada = await getDistributionConfig(intTemporadasId);
  const conn  = await connFunc.createConection();

  const presupuestoTemporadaRes = await conn
    .request()
    .input('intTemporadasId', sql.Int, intTemporadasId)
    .query(`select * from gesPresupuestos where intTemporadasId = @intTemporadasId;`);

  const presupuestoTemporada = presupuestoTemporadaRes.recordset || [];

  // Mapa: intCentrosCostosCodigoOrigen -> destinos[]
  const distribucionMap = new Map(
    (factoresTemporada ?? []).map(f => [
      f.intCentrosCostosCodigoOrigen,
      f.destinos || []
    ])
  );

  const presupuestoDistribuido = presupuestoTemporada.flatMap(presupuesto => {

    const origen   = String(presupuesto.intCentrosCostosCodigo ?? '').trim();
    const destinos = distribucionMap.get(origen);

    //const baseValor = Number(presupuesto.valor ?? 0);
    const baseValorTotal = Number(presupuesto.valorTotal ?? 0);
    const baseCantidad = Number(presupuesto.cantidad ?? 0);

    // Sin distribución → se deja tal cual
    if (!destinos || destinos.length === 0) {
      const sinDistribuir = {
        ...presupuesto,
        intCentrosCostosCodigo: origen,
        //valor: baseValor,
        valorTotal: baseValorTotal,
        cantidad: baseCantidad
      };
      return [sinDistribuir]; // ✅ importante para flatMap
    }

    // Con distribución → se abre por cada destino
    return destinos.map(d => ({
      ...presupuesto,
      intCentrosCostosCodigo: String(d.intCentrosCostosCodigoDestino ?? '').trim(),
      //valor: baseValor * Number(d.factor ?? 0),
      valorTotal: baseValorTotal * Number(d.factor ?? 0),
      cantidad: baseCantidad * Number(d.factor ?? 0)
    }));
  });

  return presupuestoDistribuido;

}

//Load functions

module.exports = {

    getTemporadas,
    distribuirGastos,
    distribuirPresupuesto,

}