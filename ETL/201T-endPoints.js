const O_endPoints = require('./401O-endPoints');
const fs = require("node:fs");

//endPoints

async function transformPresupuesto(presupuesto) {

  const items = Array.isArray(presupuesto) ? presupuesto : [];

  if (items.length === 0) return [];

  // Obtener temporadas
  const temporadas = await O_endPoints.getTemporadas();

  // Normalizar fechas de temporadas una sola vez
  const temporadasParsed = temporadas.map(t => ({
    intTemporadasId: t.intTemporadasId,
    fechaInicio: new Date(t.fechaInicio),
    fechaTermino: new Date(t.fechaTermino)
  }));

  const presupuestoConverted = items.map(item => {

    const fechaItem = new Date(item.fecha);

    const temporada = temporadasParsed.find(t =>
      fechaItem >= t.fechaInicio && fechaItem <= t.fechaTermino
    );

    return {
      intTemporadasId: temporada ? temporada.intTemporadasId : null,
      ...item
    };
  });

  return presupuestoConverted;
}

async function transformGastos(gastos) {

  const items = Array.isArray(gastos) ? gastos : [];

  if (items.length === 0) return [];

  // Obtener temporadas
  const temporadas = await O_endPoints.getTemporadas();

  // Normalizar fechas de temporadas
  const temporadasParsed = temporadas.map(t => ({
    intTemporadasId: t.intTemporadasId,
    fechaInicio: new Date(t.fechaInicio),
    fechaTermino: new Date(t.fechaTermino)
  }));

  const gastosConverted = items.map(item => {

    const fechaItem = new Date(item.fecha);

    const temporada = temporadasParsed.find(t =>
      fechaItem >= t.fechaInicio && fechaItem <= t.fechaTermino
    );

    return {
      intTemporadasId: temporada ? temporada.intTemporadasId : null,
      ...item
    };
  });

  return gastosConverted;
};

async function transformCostoMO(gastoMO) {

    const rows = Array.isArray(gastoMO) ? gastoMO : [];

    if (rows.length === 0) return [];

    const temporadasId = await O_endPoints.getTemporadas();

    const temporadasNormalized = (temporadasId ?? []).map(t => ({
      intTemporadasId: t.intTemporadasId,
      fechaInicio:     new Date(t.fechaInicio),
      fechaTermino:    new Date(t.fechaTermino),
    }));
 
    // Helpers internos

    function addRutDash(rut) {
      const s = (rut ?? '').toString().trim();
      if (!s) return null;
      if (s.includes('-')) return s;
      return s.slice(0, -1) + '-' + s.slice(-1);
    }

    function findTemporadaId(ano, mes) {

      if (!ano || !mes) return null;
      const mm = String(mes).padStart(2, '0');
      const fecha = new Date(`${ano}-${mm}-01T00:00:00`);
      for (const t of temporadasNormalized) {
        if (fecha >= t.fechaInicio && fecha <= t.fechaTermino) {
          return t.intTemporadasId;
        }
      }
      return null;

    }

    const toStr = (v) => v === null || v === undefined ? null : String(v).trim();
    const toDec = (v) => v === null || v === undefined || v === '' ? 0 : Number(v);
    const isBonificacion = (tipo) => tipo === 'tratos' || tipo === 'bonos';

    // Transformación

    const resultado = rows.map((r) => {

      const intTemporadasId = findTemporadaId(r?.ano, r?.mes);
      const bonif = isBonificacion(r?.tipoConcepto);

      return {

        intEmpresasCodigo:       addRutDash(r?.rEmpresa),
        intSucursalesCodigo:     toStr(r?.cSucursal),
        intCentrosCostosCodigo:  toStr(r?.cCentroCosto),
        intLaboresCodigo:        toStr(r?.cLabor),
        intItemsCodigo:          toStr(r?.cItem),
        remTrabajadoresCodigo:   addRutDash(r?.rTrabajador),
        remContratosTipoCodigo:  toStr(r?.cTipoContrato),
        remCargosCodigo:         toStr(r?.cCargo),
        remConceptosCodigo:      toStr(r?.cConcepto),
        remBonificacionesCodigo: bonif ? toStr(r?.cTipoConcepto)    : null,
        remBonificacionesTipo:   bonif ? toStr(r?.tipoConcepto) : null,
        intTeporadasId:          intTemporadasId,
        ano:                     Number(r?.ano),
        mes:                     Number(r?.mes),
        tipoRegistro:            toStr(r?.TipoRegistro),
        jornadas:                toDec(r?.jornadas),
        rendimiento:             toDec(r?.rendimiento),
        horasExtras:             toDec(r?.horasExtras),
        valorAsignacion:         toDec(r?.valorAsignacion),
        ajusteLiquidacion:       toDec(r?.ajusteLiquidacion),
        valorLiquidacion:        toDec(r?.valorConceptoLiquidacion),

      };

    });

    // Guardar JSON
    fs.writeFileSync('./jsonFiles/costoMOConverted.json', JSON.stringify(resultado, null, 2),'utf8' );

    return resultado;

};

async function transformCostoMOContratista(gastoMOcontratista) {

  const rows = Array.isArray(gastoMOcontratista) ? gastoMOcontratista : [];

  if (rows.length === 0) return [];

  const temporadasId = await O_endPoints.getTemporadas();

  const temporadasNormalized = (temporadasId ?? []).map(t => ({
    intTemporadasId: t.intTemporadasId,
    fechaInicio: new Date(t.fechaInicio),
    fechaTermino: new Date(t.fechaTermino),
  }));

  // Helpers internos

  function addRutDash(rut) {
    const s = (rut ?? '').toString().trim();
    if (!s) return null;
    if (s.includes('-')) return s;
    return s.slice(0, -1) + '-' + s.slice(-1);
  }

  function findTemporadaId(ano, mes) {
    if (!ano || !mes) return null;
    const mm = String(mes).padStart(2, '0');
    const fecha = new Date(`${ano}-${mm}-01T00:00:00`);

    for (const t of temporadasNormalized) {
      if (fecha >= t.fechaInicio && fecha <= t.fechaTermino) {
        return t.intTemporadasId;
      }
    }
    return null;
  }

  const toStr = (v) => v === null || v === undefined ? null : String(v).trim();
  const toDec = (v) => v === null || v === undefined || v === '' ? 0 : Number(v);

  const resultado = [];

  for (const r of rows) {

    const intTemporadasId = findTemporadaId(r?.ano, r?.mes);

    const tieneTrato =
      toStr(r?.cTrato) !== null ||
      toStr(r?.conceptoTrato) !== null ||
      toDec(r?.valorTrato) !== 0 ||
      toDec(r?.valorTratoCorregido) !== 0;

    const tieneBono =
      toStr(r?.cBono) !== null ||
      toStr(r?.conceptoBono) !== null ||
      toDec(r?.valorBono) !== 0 ||
      toDec(r?.valorBonoCorregido) !== 0;

    function buildBase() {
      return {
        intEmpresasCodigo:       addRutDash(r?.cEmpresa),
        intSucursalesCodigo:     toStr(r?.cSucursal),
        intCentrosCostosCodigo:  toStr(r?.cCentroCosto),
        intLaboresCodigo:        toStr(r?.cLabor),
        intItemsCodigo:          toStr(r?.cItem),
        remTrabajadoresCodigo:   addRutDash(r?.rTrabajador),
        remContratosTipoCodigo:  toStr(r?.cTipoContrato),
        remCargosCodigo:         toStr(r?.cCargo),
        intTeporadasId:          intTemporadasId,
        ano:                     Number(r?.ano),
        mes:                     Number(r?.mes),
        tipoRegistro:            toStr(r?.tipoRegistro ?? r?.TipoRegistro),
        horasExtras:             toDec(r?.horasExtras),
      };
    }

    // TRATO
    if (tieneTrato) {
      const valorAsignacion = toDec(r?.valorTrato);
      const valorLiquidacion = toDec(r?.valorTratoCorregido);

      resultado.push({
        ...buildBase(),
        remConceptosCodigo:      toStr(r?.conceptoTrato),
        remBonificacionesCodigo: toStr(r?.cTrato),
        remBonificacionesTipo:   'tratos',
        jornadas:                toDec(r?.jornadas),
        rendimiento:             toDec(r?.rendimiento),
        valorAsignacion,
        ajusteLiquidacion:       valorLiquidacion - valorAsignacion,
        valorLiquidacion,
      });
    }

    // BONO
    if (tieneBono) {
      const valorAsignacion = toDec(r?.valorBono);
      const valorLiquidacion = toDec(r?.valorBonoCorregido);

      resultado.push({
        ...buildBase(),
        remConceptosCodigo:      toStr(r?.conceptoBono),
        remBonificacionesCodigo: toStr(r?.cBono),
        remBonificacionesTipo:   'bonos',
        jornadas:                tieneTrato ? 0 : toDec(r?.jornadas),
        rendimiento:             tieneTrato ? 0 : toDec(r?.rendimiento),
        valorAsignacion,
        ajusteLiquidacion:       valorLiquidacion - valorAsignacion,
        valorLiquidacion,
      });
    }
  }

  fs.writeFileSync(
    './jsonFiles/costoMOContratistaConverted.json',
    JSON.stringify(resultado, null, 2),
    'utf8'
  );

  return resultado;
}


module.exports = {

    transformPresupuesto,
    transformGastos,
    transformCostoMO,
    transformCostoMOContratista,

};