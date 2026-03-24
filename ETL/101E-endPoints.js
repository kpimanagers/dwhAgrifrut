require('dotenv').config();
const axios = require('axios');
const { getConnectorCredentials } = require('../auth/apiCredentials');
const connFunc = require('../db/conexion');
const fs = require("node:fs");

const baseUrl = process.env.BASE_URL;

//endPoints estructuras

async function getEmpresas() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/empresas`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });

    return resp.data

};

async function getSucursales() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/sucursales`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getPredios() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/predios`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getCuarteles() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/cuarteles`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getCentrosCosto() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/centrosCosto`;
  
  const resp = await axios.get(url, {
  
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getCentrosCostoDistribucion() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/centrosCostoDistribucion`;
  
  const resp = await axios.get(url, {
  
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getEspecies() {

    const creds = await getConnectorCredentials();
    const {  apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/especies`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getVariedades() {

    const creds = await getConnectorCredentials();
    const {  apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/variedades`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getFaenas() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/faenas`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getLabores() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/labores`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getSuperFamilias() {

    const creds = await getConnectorCredentials();
    const {  apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/superFamilias`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getFamilias() {

    const creds = await getConnectorCredentials();
    const {  apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/familias`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getSubFamilias() {

    const creds = await getConnectorCredentials();
    const {  apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/subFamilias`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getItems() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/items`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getUnidades() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/unidades`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

//endPoints gestión

async function getPresupuestos() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/presupuestos`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    return resp.data;

};

async function getGastosSinDistribuir(fechaDesde) {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/gastosSinDistribuir`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    },
    params: {fechaDesde}
  
  });
  
    return resp.data;

};

//endPoints Mano de Obra

async function getTrabajadores() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/trabajadores`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
  return resp.data;

};

async function getTiposContrato() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;

    const url = `${baseUrl}/api/tiposContrato`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
  return resp.data;

};

async function getCargos() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/cargos`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
  return resp.data;

};

async function getConceptos() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/conceptos`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
  return resp.data;

};

async function getBonificaciones() {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/bonificaciones`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
  return resp.data;

};

async function getRegistroMO(ano,mes) {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;

    const url = `${baseUrl}/api/registroMO`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    },
    params: {ano,mes}
  
  });

    fs.writeFileSync('./jsonFiles/RegistroMO.json', JSON.stringify(resp.data, null, 2), 'utf8');
  
    return resp.data;

};

async function getLiquidaciones(ano,mes) {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/liquidaciones`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    },
    params: {ano,mes}
  
  });

    fs.writeFileSync('./jsonFiles/Liquidaciones.json', JSON.stringify(resp.data, null, 2), 'utf8');
  
    return resp.data;

};

async function getCostoMOOLD(ano, mes) {

  try {
    const registrosMO   = await getRegistroMO(ano, mes);
    const liquidaciones = await getLiquidaciones(ano, mes);

    const EPS = 0.000001;
 
    // Normaliza RUT para matching (MO trae con guion, Liquidaciones sin guion)
    const rutKey = (v) => {
      const s = (v ?? '').toString().trim().toUpperCase();
      if (!s) return '';
      return s.replace(/\./g, '').replace(/-/g, '').replace(/\s+/g, '');
    };

    // OJO: ahora incluye empresa (para no mezclar por empresa)
    const workerKey = (ano, mes, empresa, idContrato, rTrabajador) =>
      `${ano}|${mes}|${rutKey(empresa)}|${idContrato}|${rutKey(rTrabajador)}`;

    // Indexar MO por trabajador (incluye empresa normalizada)
    const moByWorker = new Map();
    for (const r of registrosMO) {
      const k = workerKey(r.ano, r.mes, r.cEmpresa, r.idContrato, r.rTrabajador);
      if (!moByWorker.has(k)) moByWorker.set(k, []);
      moByWorker.get(k).push(r);
    }

    // Indexar liquidaciones por trabajador y concepto (incluye empresa normalizada)
    const liqByWorker = new Map();
    for (const r of liquidaciones) {
      const k = workerKey(r.ano, r.mes, r.rEmpresa, r.idContrato, r.rTrabajador);
      if (!liqByWorker.has(k)) liqByWorker.set(k, new Map());
      const conceptMap = liqByWorker.get(k);

      const cConcepto = r.cConcepto || 'SIN_CONCEPTO';
      if (!conceptMap.has(cConcepto)) {
        conceptMap.set(cConcepto, {
          ano: r.ano,
          mes: r.mes,
          idContrato: r.idContrato,
          rEmpresa: r.rEmpresa,      // se mantiene desde Liquidaciones
          nEmpresa: r.nEmpresa,
          rTrabajador: r.rTrabajador,
          nTrabajador: r.nTrabajador,
          cTipoContrato: r.cTipoContrato,
          nTipoContrato: r.nTipoContrato,
          TipoRegistro: r.TipoRegistro,
          cCargo: r.cCargo,
          nCargo: r.nCargo,
          cConcepto: r.cConcepto,
          nConcepto: r.nConcepto,
          cItem: r.cItem,
          nItem: r.nItem,
          costoEmpresa: r.costoEmpresa,
          totalConcepto: 0
        });
      }
      const obj = conceptMap.get(cConcepto);
      obj.totalConcepto += Number(r.valor) || 0;
    }

    const resultado = [];

    // Procesar trabajador por trabajador
    for (const [wKey, conceptMap] of liqByWorker.entries()) {
      const moRows = moByWorker.get(wKey) || [];
      const conceptCodesLiq = Array.from(conceptMap.keys());

      if (moRows.length === 0) {
        // Sin MO: no hay forma de asignar sucursal -> null
        for (const c of conceptCodesLiq) {
          const info = conceptMap.get(c);
          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: null, // <-- no existe MO para inferirla

            cCentroCosto: null,
            nCentroCosto: null,
            cLabor: null,
            nLabor: null,
            tipoConcepto: 'sinMO',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,
            cTipoConcepto: null,
            nTipoConcepto: null,
            jornadas: null,
            rendimiento: null,
            horasExtras: null,
            valorAsignacion: 0,
            ajusteLiquidacion: info.totalConcepto,
            costoFinal: info.totalConcepto,
            valorConceptoLiquidacion: info.totalConcepto
          });
        }
        continue;
      }

      // Conjuntos de conceptos según MO
      const tratoConcepts = new Set(moRows.map(r => r.conceptoTrato).filter(Boolean));
      const bonoConcepts  = new Set(moRows.map(r => r.conceptoBono).filter(Boolean));
      const heConceptsMO  = new Set(moRows.map(r => r.conceptoHe).filter(Boolean));

      const heConcepts           = conceptCodesLiq.filter(c => heConceptsMO.has(c));
      const tratoConceptsInLiq   = conceptCodesLiq.filter(c => tratoConcepts.has(c));
      const bonoConceptsInLiq    = conceptCodesLiq.filter(c => bonoConcepts.has(c));
      const unionTratoBono       = new Set([...tratoConceptsInLiq, ...bonoConceptsInLiq]);

      const otherConcepts = conceptCodesLiq.filter(
        c => !unionTratoBono.has(c) && !heConcepts.includes(c)
      );

      // 2) Otros conceptos por jornadas
      const totalJornadas = moRows.reduce((acc, r) => acc + (Number(r.jornadas) || 0), 0);
      const baseJornadas = totalJornadas === 0 ? moRows.length : totalJornadas;

      for (const c of otherConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        for (const r of moRows) {
          const baseVal = Number(r.jornadas) || 0;
          const proporcion = (totalJornadas === 0) ? (1 / baseJornadas) : (baseVal / totalJornadas);

          const valorAsignacion = 0;
          const ajusteLiquidacion = totalConcepto * proporcion;
          const costoFinal = valorAsignacion + ajusteLiquidacion;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa, // Liquidaciones
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null, // <-- MO

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'otros',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: null,
            nTipoConcepto: null,

            jornadas: r.jornadas || 0,
            rendimiento: r.rendimiento || 0,
            horasExtras: r.horasExtras || 0,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // 3) HE por horasExtras
      for (const c of heConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsHE = moRows.filter(r => (r.horasExtras || 0) > 0);
        const baseRows = rowsHE.length > 0 ? rowsHE : moRows;

        const totalHE = baseRows.reduce((acc, r) => acc + (Number(r.horasExtras) || 0), 0);
        const baseHE = totalHE === 0 ? baseRows.length : totalHE;

        for (const r of baseRows) {
          const baseVal = Number(r.horasExtras) || 0;
          const proporcion = (totalHE === 0) ? (1 / baseHE) : (baseVal / totalHE);

          const valorAsignacion = 0;
          const ajusteLiquidacion = totalConcepto * proporcion;
          const costoFinal = valorAsignacion + ajusteLiquidacion;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null, // <-- MO

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'horasExtras',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: null,
            nTipoConcepto: null,

            jornadas: r.jornadas || 0,
            rendimiento: r.rendimiento || 0,
            horasExtras: r.horasExtras || 0,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // 4–7) Tratos y Bonos
      const tratoConceptsInLiq2 = conceptCodesLiq.filter(c => tratoConcepts.has(c));
      const bonoConceptsInLiq2  = conceptCodesLiq.filter(c => bonoConcepts.has(c));

      const allTratoConcepts = new Set(tratoConceptsInLiq2);
      const allBonoConcepts  = new Set(bonoConceptsInLiq2);

      const sharedConcepts = conceptCodesLiq.filter(c => allTratoConcepts.has(c) && allBonoConcepts.has(c));
      const tratoOnlyConcepts = conceptCodesLiq.filter(c => allTratoConcepts.has(c) && !allBonoConcepts.has(c));
      const bonoOnlyConcepts  = conceptCodesLiq.filter(c => allBonoConcepts.has(c) && !allTratoConcepts.has(c));

      // Tratos solo
      for (const c of tratoOnlyConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsTrato = moRows.filter(r => r.conceptoTrato === c);
        if (rowsTrato.length === 0) continue;

        const totalAsignTrato = rowsTrato.reduce((acc, r) => acc + (Number(r.valorTrato) || 0), 0);
        const diferencia = totalConcepto - totalAsignTrato;

        const totalBase = rowsTrato.reduce((acc, r) => acc + (Number(r.rendimiento) || 0), 0);
        const baseTotal = totalBase === 0 ? rowsTrato.length : totalBase;

        for (const r of rowsTrato) {
          const baseVal = Number(r.rendimiento) || 0;
          const proporcion = (totalBase === 0) ? (1 / baseTotal) : (baseVal / totalBase);

          const valorAsignacion = Number(r.valorTrato) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diferencia) > EPS) ajusteLiquidacion = diferencia * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null, // <-- MO

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'tratos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cTrato || null,
            nTipoConcepto: r.nTrato || null,

            jornadas: r.jornadas || 0,
            rendimiento: r.rendimiento || 0,
            horasExtras: r.horasExtras || 0,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // Bonos solo
      for (const c of bonoOnlyConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsBono = moRows.filter(r => r.conceptoBono === c);
        if (rowsBono.length === 0) continue;

        const totalAsignBono = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);
        const diferencia = totalConcepto - totalAsignBono;

        const totalBase = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);
        const baseTotal = totalBase === 0 ? rowsBono.length : totalBase;

        for (const r of rowsBono) {
          const baseVal = Number(r.valorBono) || 0;
          const proporcion = (totalBase === 0) ? (1 / baseTotal) : (baseVal / totalBase);

          const valorAsignacion = Number(r.valorBono) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diferencia) > EPS) ajusteLiquidacion = diferencia * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null, // <-- MO

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'bonos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cBono || null,
            nTipoConcepto: r.nBono || null,

            jornadas: r.jornadas,
            rendimiento: r.rendimiento || 0,
            horasExtras: r.horasExtras || 0,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // Conceptos compartidos Trato+Bono
      for (const c of sharedConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsTrato = moRows.filter(r => r.conceptoTrato === c);
        const rowsBono  = moRows.filter(r => r.conceptoBono === c);

        const totalAsignTrato = rowsTrato.reduce((acc, r) => acc + (Number(r.valorTrato) || 0), 0);
        const totalAsignBono  = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);

        const sumaAsign = totalAsignTrato + totalAsignBono;
        const diferenciaTotal = totalConcepto - sumaAsign;

        let diffTrato = 0;
        let diffBono  = 0;

        if (Math.abs(diferenciaTotal) > EPS && sumaAsign !== 0) {
          diffTrato = diferenciaTotal * (totalAsignTrato / sumaAsign);
          diffBono  = diferenciaTotal * (totalAsignBono / sumaAsign);
        }

        // Tratos: prorrateo por rendimiento
        const totalBaseTrato = rowsTrato.reduce((acc, r) => acc + (Number(r.rendimiento) || 0), 0);
        const baseTrato = totalBaseTrato === 0 ? rowsTrato.length : totalBaseTrato;

        for (const r of rowsTrato) {
          const baseVal = Number(r.rendimiento) || 0;
          const proporcion = (totalBaseTrato === 0) ? (1 / baseTrato) : (baseVal / totalBaseTrato);

          const valorAsignacion = Number(r.valorTrato) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diffTrato) > EPS) ajusteLiquidacion = diffTrato * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null, // <-- MO

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'tratos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cTrato || null,
            nTipoConcepto: r.nTrato || null,

            jornadas: r.jornadas || 0,
            rendimiento: r.rendimiento || 0,
            horasExtras: r.horasExtras || 0,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }

        // Bonos: prorrateo por valor bono
        const totalBaseBono = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);
        const baseBono = totalBaseBono === 0 ? rowsBono.length : totalBaseBono;

        for (const r of rowsBono) {
          const baseVal = Number(r.valorBono) || 0;
          const proporcion = (totalBaseBono === 0) ? (1 / baseBono) : (baseVal / totalBaseBono);

          const valorAsignacion = Number(r.valorBono) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diffBono) > EPS) ajusteLiquidacion = diffBono * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null, // <-- MO

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'bonos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cBono || null,
            nTipoConcepto: r.nBono || null,

            jornadas: r.jornadas,
            rendimiento: r.rendimiento || 0,
            horasExtras: r.horasExtras || 0,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }
    }

    fs.writeFileSync('./jsonFiles/costoMO.json', JSON.stringify(resultado, null, 2), 'utf8');
    return resultado;

  } catch (err) {
    console.error('Error en transformCostoMO:', err);
    throw err;
  }
};

async function getCostoMO(ano, mes) {

  try {
    const registrosMO   = await getRegistroMO(ano, mes);
    const liquidaciones = await getLiquidaciones(ano, mes);

    const EPS = 0.000001;

    // Normaliza RUT para matching (MO trae con guion, Liquidaciones sin guion)
    const rutKey = (v) => {
      const s = (v ?? '').toString().trim().toUpperCase();
      if (!s) return '';
      return s.replace(/\./g, '').replace(/-/g, '').replace(/\s+/g, '');
    };

    // OJO: ahora incluye empresa (para no mezclar por empresa)
    const workerKey = (ano, mes, empresa, idContrato, rTrabajador) =>
      `${ano}|${mes}|${rutKey(empresa)}|${idContrato}|${rutKey(rTrabajador)}`;

    // Indexar MO por trabajador (incluye empresa normalizada)
    const moByWorker = new Map();
    for (const r of registrosMO) {
      const k = workerKey(r.ano, r.mes, r.cEmpresa, r.idContrato, r.rTrabajador);
      if (!moByWorker.has(k)) moByWorker.set(k, []);
      moByWorker.get(k).push(r);
    }

    // Indexar liquidaciones por trabajador y concepto (incluye empresa normalizada)
    const liqByWorker = new Map();
    for (const r of liquidaciones) {
      const k = workerKey(r.ano, r.mes, r.rEmpresa, r.idContrato, r.rTrabajador);
      if (!liqByWorker.has(k)) liqByWorker.set(k, new Map());
      const conceptMap = liqByWorker.get(k);

      const cConcepto = r.cConcepto || 'SIN_CONCEPTO';
      if (!conceptMap.has(cConcepto)) {
        conceptMap.set(cConcepto, {
          ano: r.ano,
          mes: r.mes,
          idContrato: r.idContrato,
          rEmpresa: r.rEmpresa,
          nEmpresa: r.nEmpresa,
          rTrabajador: r.rTrabajador,
          nTrabajador: r.nTrabajador,
          cTipoContrato: r.cTipoContrato,
          nTipoContrato: r.nTipoContrato,
          TipoRegistro: r.TipoRegistro,
          cCargo: r.cCargo,
          nCargo: r.nCargo,
          cConcepto: r.cConcepto,
          nConcepto: r.nConcepto,
          cItem: r.cItem,
          nItem: r.nItem,
          costoEmpresa: r.costoEmpresa,
          totalConcepto: 0
        });
      }

      const obj = conceptMap.get(cConcepto);
      obj.totalConcepto += Number(r.valor) || 0;
    }

    const resultado = [];

    // Helper: evita duplicación de métricas MO al explotar por concepto
    const resolveMetricOwners = ({
      r,
      hasTratoOrBonoInLiq,
      hasTratoInLiq,
      hasBonoInLiq,
      hasHeInLiq,
      conceptMap,
      otherConcepts,
      heConcepts
    }) => {
      const conceptoTrato = r?.conceptoTrato || null;
      const conceptoBono  = r?.conceptoBono  || null;
      const conceptoHe    = r?.conceptoHe    || null;

      const tratoExistsInLiq = !!(conceptoTrato && conceptMap.has(conceptoTrato));
      const bonoExistsInLiq  = !!(conceptoBono  && conceptMap.has(conceptoBono));
      const heExistsInLiq    = !!(conceptoHe    && conceptMap.has(conceptoHe));

      // 1) Jornadas + rendimiento:
      // - Si hay trato en LIQ => dueño trato (y si hay trato+bono en mismo registro, igual manda trato)
      // - Si no hay trato pero hay bono en LIQ => dueño bono
      // - Si no hay tratos ni bonos en LIQ => dueño = 1 solo concepto "otros" (primer otherConcept), y rendimiento SIEMPRE 0
      let jrOwnerType = null;
      let jrOwnerConcept = null;

      if (hasTratoOrBonoInLiq) {
        if (tratoExistsInLiq) {
          jrOwnerType = 'tratos';
          jrOwnerConcept = conceptoTrato;
        } else if (bonoExistsInLiq) {
          jrOwnerType = 'bonos';
          jrOwnerConcept = conceptoBono;
        } else {
          // Hay tratos/bonos en LIQ para el trabajador, pero este registro MO en particular no calza con ningún concepto.
          // Para no duplicar, lo mandamos a 1 solo "otros" si existe.
          jrOwnerType = otherConcepts.length ? 'otros' : null;
          jrOwnerConcept = otherConcepts.length ? otherConcepts[0] : null;
        }
      } else {
        // Regla: sin tratos ni bonos => no prorratear jornadas por concepto y rendimiento = 0
        jrOwnerType = otherConcepts.length ? 'otros' : null;
        jrOwnerConcept = otherConcepts.length ? otherConcepts[0] : null;
      }

      // 2) Horas extras:
      // - Si existe concepto HE en LIQ => dueño = conceptoHe (o si no está, primer heConcept disponible)
      // - Si NO existe concepto HE en LIQ => se pegan al dueño de jornadas (trato/bono/otros)
      let heOwnerType = null;
      let heOwnerConcept = null;

      if ((Number(r?.horasExtras) || 0) > 0) {
        if (hasHeInLiq && heConcepts.length) {
          heOwnerType = 'horasExtras';
          if (heExistsInLiq && heConcepts.includes(conceptoHe)) {
            heOwnerConcept = conceptoHe;
          } else {
            // fallback: primer HE en liquidación
            heOwnerConcept = heConcepts[0];
          }
        } else {
          // No hay HE en liquidación: no creamos "otro concepto HE" artificial,
          // solo evitamos perder la métrica pegándola al dueño principal.
          heOwnerType = jrOwnerType;
          heOwnerConcept = jrOwnerConcept;
        }
      }

      return { jrOwnerType, jrOwnerConcept, heOwnerType, heOwnerConcept };
    };

    // Procesar trabajador por trabajador
    for (const [wKey, conceptMap] of liqByWorker.entries()) {
      const moRows = moByWorker.get(wKey) || [];
      const conceptCodesLiq = Array.from(conceptMap.keys());

      if (moRows.length === 0) {
        // Sin MO: no hay forma de asignar sucursal -> null
        for (const c of conceptCodesLiq) {
          const info = conceptMap.get(c);
          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: null,

            cCentroCosto: null,
            nCentroCosto: null,
            cLabor: null,
            nLabor: null,

            tipoConcepto: 'sinMO',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: null,
            nTipoConcepto: null,

            jornadas: null,
            rendimiento: null,
            horasExtras: null,

            valorAsignacion: 0,
            ajusteLiquidacion: info.totalConcepto,
            costoFinal: info.totalConcepto,
            valorConceptoLiquidacion: info.totalConcepto
          });
        }
        continue;
      }

      // Conjuntos de conceptos según MO
      const tratoConcepts = new Set(moRows.map(r => r.conceptoTrato).filter(Boolean));
      const bonoConcepts  = new Set(moRows.map(r => r.conceptoBono).filter(Boolean));
      const heConceptsMO  = new Set(moRows.map(r => r.conceptoHe).filter(Boolean));

      const heConcepts           = conceptCodesLiq.filter(c => heConceptsMO.has(c));
      const tratoConceptsInLiq   = conceptCodesLiq.filter(c => tratoConcepts.has(c));
      const bonoConceptsInLiq    = conceptCodesLiq.filter(c => bonoConcepts.has(c));
      const unionTratoBono       = new Set([...tratoConceptsInLiq, ...bonoConceptsInLiq]);

      const otherConcepts = conceptCodesLiq.filter(
        c => !unionTratoBono.has(c) && !heConcepts.includes(c)
      );

      const hasTratoInLiq = tratoConceptsInLiq.length > 0;
      const hasBonoInLiq  = bonoConceptsInLiq.length > 0;
      const hasTratoOrBonoInLiq = hasTratoInLiq || hasBonoInLiq;
      const hasHeInLiq = heConcepts.length > 0;

      // 2) Otros conceptos: mantiene tu lógica de ajusteLiquidacion (prorrateo por jornadas),
      // PERO las métricas MO (jornadas/rendimiento/HE) se registran una sola vez según "dueños".
      const totalJornadas = moRows.reduce((acc, r) => acc + (Number(r.jornadas) || 0), 0);
      const baseJornadas = totalJornadas === 0 ? moRows.length : totalJornadas;

      for (const c of otherConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        for (const r of moRows) {
          const baseVal = Number(r.jornadas) || 0;
          const proporcion = (totalJornadas === 0) ? (1 / baseJornadas) : (baseVal / totalJornadas);

          const valorAsignacion = 0;
          const ajusteLiquidacion = totalConcepto * proporcion;
          const costoFinal = valorAsignacion + ajusteLiquidacion;

          const owners = resolveMetricOwners({
            r,
            hasTratoOrBonoInLiq,
            hasTratoInLiq,
            hasBonoInLiq,
            hasHeInLiq,
            conceptMap,
            otherConcepts,
            heConcepts
          });

          const jornadasOut =
            (owners.jrOwnerType === 'otros' && owners.jrOwnerConcept === c)
              ? (Number(r.jornadas) || 0)
              : 0;

          // Regla: sin tratos ni bonos => rendimiento = 0 (aunque el MO lo traiga)
          const rendimientoOut =
            (!hasTratoOrBonoInLiq)
              ? 0
              : ((owners.jrOwnerType === 'otros' && owners.jrOwnerConcept === c)
                  ? (Number(r.rendimiento) || 0)
                  : 0);

          // HE solo acá si NO hay concepto HE en liquidación y el dueño quedó pegado a "otros"
          const horasExtrasOut =
            (!hasHeInLiq && owners.heOwnerType === 'otros' && owners.heOwnerConcept === c)
              ? (Number(r.horasExtras) || 0)
              : 0;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null,

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'otros',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: null,
            nTipoConcepto: null,

            jornadas: jornadasOut,
            rendimiento: rendimientoOut,
            horasExtras: horasExtrasOut,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // 3) HE por horasExtras (ajusteLiquidacion proporcional a horasExtras, como ya hacías),
      // y las métricas MO solo se anotan aquí (no en otros/tratos/bonos) si existe concepto HE en liquidación.
      for (const c of heConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsHE = moRows.filter(r => (Number(r.horasExtras) || 0) > 0);
        const baseRows = rowsHE.length > 0 ? rowsHE : moRows;

        const totalHE = baseRows.reduce((acc, r) => acc + (Number(r.horasExtras) || 0), 0);
        const baseHE = totalHE === 0 ? baseRows.length : totalHE;

        for (const r of baseRows) {
          const baseVal = Number(r.horasExtras) || 0;
          const proporcion = (totalHE === 0) ? (1 / baseHE) : (baseVal / totalHE);

          const valorAsignacion = 0;
          const ajusteLiquidacion = totalConcepto * proporcion;
          const costoFinal = valorAsignacion + ajusteLiquidacion;

          const owners = resolveMetricOwners({
            r,
            hasTratoOrBonoInLiq,
            hasTratoInLiq,
            hasBonoInLiq,
            hasHeInLiq,
            conceptMap,
            otherConcepts,
            heConcepts
          });

          const horasExtrasOut =
            (owners.heOwnerType === 'horasExtras' && owners.heOwnerConcept === c)
              ? (Number(r.horasExtras) || 0)
              : 0;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null,

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'horasExtras',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: null,
            nTipoConcepto: null,

            // Importante: NO duplicar jornadas/rendimiento en HE
            jornadas: 0,
            rendimiento: 0,
            horasExtras: horasExtrasOut,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // 4–7) Tratos y Bonos
      const tratoConceptsInLiq2 = conceptCodesLiq.filter(c => tratoConcepts.has(c));
      const bonoConceptsInLiq2  = conceptCodesLiq.filter(c => bonoConcepts.has(c));

      const allTratoConcepts = new Set(tratoConceptsInLiq2);
      const allBonoConcepts  = new Set(bonoConceptsInLiq2);

      const sharedConcepts = conceptCodesLiq.filter(c => allTratoConcepts.has(c) && allBonoConcepts.has(c));
      const tratoOnlyConcepts = conceptCodesLiq.filter(c => allTratoConcepts.has(c) && !allBonoConcepts.has(c));
      const bonoOnlyConcepts  = conceptCodesLiq.filter(c => allBonoConcepts.has(c) && !allTratoConcepts.has(c));

      // Tratos solo
      for (const c of tratoOnlyConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsTrato = moRows.filter(r => r.conceptoTrato === c);
        if (rowsTrato.length === 0) continue;

        const totalAsignTrato = rowsTrato.reduce((acc, r) => acc + (Number(r.valorTrato) || 0), 0);
        const diferencia = totalConcepto - totalAsignTrato;

        const totalBase = rowsTrato.reduce((acc, r) => acc + (Number(r.rendimiento) || 0), 0);
        const baseTotal = totalBase === 0 ? rowsTrato.length : totalBase;

        for (const r of rowsTrato) {
          const baseVal = Number(r.rendimiento) || 0;
          const proporcion = (totalBase === 0) ? (1 / baseTotal) : (baseVal / totalBase);

          const valorAsignacion = Number(r.valorTrato) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diferencia) > EPS) ajusteLiquidacion = diferencia * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          const owners = resolveMetricOwners({
            r,
            hasTratoOrBonoInLiq,
            hasTratoInLiq,
            hasBonoInLiq,
            hasHeInLiq,
            conceptMap,
            otherConcepts,
            heConcepts
          });

          const jornadasOut =
            (owners.jrOwnerType === 'tratos' && owners.jrOwnerConcept === c)
              ? (Number(r.jornadas) || 0)
              : 0;

          const rendimientoOut =
            (owners.jrOwnerType === 'tratos' && owners.jrOwnerConcept === c)
              ? (Number(r.rendimiento) || 0)
              : 0;

          // HE solo acá si NO hay concepto HE en liquidación y el dueño quedó pegado a trato
          const horasExtrasOut =
            (!hasHeInLiq && owners.heOwnerType === 'tratos' && owners.heOwnerConcept === c)
              ? (Number(r.horasExtras) || 0)
              : 0;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null,

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'tratos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cTrato || null,
            nTipoConcepto: r.nTrato || null,

            jornadas: jornadasOut,
            rendimiento: rendimientoOut,
            horasExtras: horasExtrasOut,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // Bonos solo
      for (const c of bonoOnlyConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsBono = moRows.filter(r => r.conceptoBono === c);
        if (rowsBono.length === 0) continue;

        const totalAsignBono = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);
        const diferencia = totalConcepto - totalAsignBono;

        const totalBase = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);
        const baseTotal = totalBase === 0 ? rowsBono.length : totalBase;

        for (const r of rowsBono) {
          const baseVal = Number(r.valorBono) || 0;
          const proporcion = (totalBase === 0) ? (1 / baseTotal) : (baseVal / totalBase);

          const valorAsignacion = Number(r.valorBono) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diferencia) > EPS) ajusteLiquidacion = diferencia * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          const owners = resolveMetricOwners({
            r,
            hasTratoOrBonoInLiq,
            hasTratoInLiq,
            hasBonoInLiq,
            hasHeInLiq,
            conceptMap,
            otherConcepts,
            heConcepts
          });

          const jornadasOut =
            (owners.jrOwnerType === 'bonos' && owners.jrOwnerConcept === c)
              ? (Number(r.jornadas) || 0)
              : 0;

          // Si hay tratos/bonos en LIQ y este registro es bono dueño => podrías querer rendimiento,
          // pero tu regla clave era: cuando NO hay tratos ni bonos => rendimiento=0.
          // Aquí hay bono, así que si el MO trae rendimiento y el dueño es bono, lo pasamos;
          // si no lo quieres nunca en bonos, cambia a 0 fijo.
          const rendimientoOut =
            (owners.jrOwnerType === 'bonos' && owners.jrOwnerConcept === c)
              ? (Number(r.rendimiento) || 0)
              : 0;

          const horasExtrasOut =
            (!hasHeInLiq && owners.heOwnerType === 'bonos' && owners.heOwnerConcept === c)
              ? (Number(r.horasExtras) || 0)
              : 0;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null,

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'bonos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cBono || null,
            nTipoConcepto: r.nBono || null,

            jornadas: jornadasOut,
            rendimiento: rendimientoOut,
            horasExtras: horasExtrasOut,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }

      // Conceptos compartidos Trato+Bono
      for (const c of sharedConcepts) {
        const info = conceptMap.get(c);
        const totalConcepto = info.totalConcepto || 0;

        const rowsTrato = moRows.filter(r => r.conceptoTrato === c);
        const rowsBono  = moRows.filter(r => r.conceptoBono === c);

        const totalAsignTrato = rowsTrato.reduce((acc, r) => acc + (Number(r.valorTrato) || 0), 0);
        const totalAsignBono  = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);

        const sumaAsign = totalAsignTrato + totalAsignBono;
        const diferenciaTotal = totalConcepto - sumaAsign;

        let diffTrato = 0;
        let diffBono  = 0;

        if (Math.abs(diferenciaTotal) > EPS && sumaAsign !== 0) {
          diffTrato = diferenciaTotal * (totalAsignTrato / sumaAsign);
          diffBono  = diferenciaTotal * (totalAsignBono / sumaAsign);
        }

        // Tratos: prorrateo por rendimiento
        const totalBaseTrato = rowsTrato.reduce((acc, r) => acc + (Number(r.rendimiento) || 0), 0);
        const baseTrato = totalBaseTrato === 0 ? rowsTrato.length : totalBaseTrato;

        for (const r of rowsTrato) {
          const baseVal = Number(r.rendimiento) || 0;
          const proporcion = (totalBaseTrato === 0) ? (1 / baseTrato) : (baseVal / totalBaseTrato);

          const valorAsignacion = Number(r.valorTrato) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diffTrato) > EPS) ajusteLiquidacion = diffTrato * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          const owners = resolveMetricOwners({
            r,
            hasTratoOrBonoInLiq,
            hasTratoInLiq,
            hasBonoInLiq,
            hasHeInLiq,
            conceptMap,
            otherConcepts,
            heConcepts
          });

          const jornadasOut =
            (owners.jrOwnerType === 'tratos' && owners.jrOwnerConcept === c)
              ? (Number(r.jornadas) || 0)
              : 0;

          const rendimientoOut =
            (owners.jrOwnerType === 'tratos' && owners.jrOwnerConcept === c)
              ? (Number(r.rendimiento) || 0)
              : 0;

          const horasExtrasOut =
            (!hasHeInLiq && owners.heOwnerType === 'tratos' && owners.heOwnerConcept === c)
              ? (Number(r.horasExtras) || 0)
              : 0;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null,

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'tratos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cTrato || null,
            nTipoConcepto: r.nTrato || null,

            jornadas: jornadasOut,
            rendimiento: rendimientoOut,
            horasExtras: horasExtrasOut,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }

        // Bonos: prorrateo por valor bono
        const totalBaseBono = rowsBono.reduce((acc, r) => acc + (Number(r.valorBono) || 0), 0);
        const baseBono = totalBaseBono === 0 ? rowsBono.length : totalBaseBono;

        for (const r of rowsBono) {
          const baseVal = Number(r.valorBono) || 0;
          const proporcion = (totalBaseBono === 0) ? (1 / baseBono) : (baseVal / totalBaseBono);

          const valorAsignacion = Number(r.valorBono) || 0;
          let ajusteLiquidacion = 0;
          if (Math.abs(diffBono) > EPS) ajusteLiquidacion = diffBono * proporcion;

          const costoFinal = valorAsignacion + ajusteLiquidacion;

          const owners = resolveMetricOwners({
            r,
            hasTratoOrBonoInLiq,
            hasTratoInLiq,
            hasBonoInLiq,
            hasHeInLiq,
            conceptMap,
            otherConcepts,
            heConcepts
          });

          // Regla clave borde: si un registro tiene trato+bono, las jornadas/rendimiento deben ir SOLO a trato.
          // Eso ya lo asegura resolveMetricOwners (prefiere trato). Por lo tanto aquí normalmente será 0.
          const jornadasOut =
            (owners.jrOwnerType === 'bonos' && owners.jrOwnerConcept === c)
              ? (Number(r.jornadas) || 0)
              : 0;

          const rendimientoOut =
            (owners.jrOwnerType === 'bonos' && owners.jrOwnerConcept === c)
              ? (Number(r.rendimiento) || 0)
              : 0;

          const horasExtrasOut =
            (!hasHeInLiq && owners.heOwnerType === 'bonos' && owners.heOwnerConcept === c)
              ? (Number(r.horasExtras) || 0)
              : 0;

          resultado.push({
            ano: info.ano,
            mes: info.mes,
            idContrato: info.idContrato,
            rEmpresa: info.rEmpresa,
            nEmpresa: info.nEmpresa,
            rTrabajador: info.rTrabajador,
            nTrabajador: info.nTrabajador,
            cTipoContrato: info.cTipoContrato,
            nTipoContrato: info.nTipoContrato,
            TipoRegistro: info.TipoRegistro,
            cCargo: info.cCargo,
            nCargo: info.nCargo,

            cSucursal: r.cSucursal ?? null,

            cCentroCosto: r.cCentroCosto,
            nCentroCosto: r.nCentroCosto,
            cLabor: r.cLabor,
            nLabor: r.nLabor,

            tipoConcepto: 'bonos',
            cConcepto: info.cConcepto,
            nConcepto: info.nConcepto,
            cItem: info.cItem,
            nItem: info.nItem,

            cTipoConcepto: r.cBono || null,
            nTipoConcepto: r.nBono || null,

            jornadas: jornadasOut,
            rendimiento: rendimientoOut,
            horasExtras: horasExtrasOut,

            valorAsignacion,
            ajusteLiquidacion,
            costoFinal,
            valorConceptoLiquidacion: totalConcepto
          });
        }
      }
    }

    fs.writeFileSync('./jsonFiles/costoMO.json', JSON.stringify(resultado, null, 2), 'utf8');
    return resultado;

  } catch (err) {
    console.error('Error en transformCostoMO:', err);
    throw err;
  }
}

async function getCostoMOdesdeFecha(fechaDesde) {

  if (!fechaDesde) return [];

  // Parse seguro en horario local para evitar el desfase UTC (YYYY-MM-DD)
  let start;
  if (fechaDesde instanceof Date) {
    start = new Date(fechaDesde.getFullYear(), fechaDesde.getMonth(), fechaDesde.getDate());
  } else if (typeof fechaDesde === "string") {
    const m = fechaDesde.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) throw new Error("Fecha inválida en getCostoMOdesdeFecha. Usa 'YYYY-MM-DD'.");
    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);
    start = new Date(y, mo - 1, d); // <- local
  } else {
    throw new Error("fechaDesde debe ser string 'YYYY-MM-DD' o Date.");
  }

  const today = new Date();

  let currentYear  = start.getFullYear();
  let currentMonth = start.getMonth() + 1;

  const endYear  = today.getFullYear();
  const endMonth = today.getMonth() + 1;

  const results = [];

  while (
    currentYear < endYear ||
    (currentYear === endYear && currentMonth <= endMonth)
  ) {

    console.log(`Procesando ${currentYear}-${String(currentMonth).padStart(2, "0")}`);

    const res = await getCostoMO(currentYear, currentMonth);
    if (res) results.push(res); // res puede ser array

    currentMonth++;
    if (currentMonth > 12) {
      currentMonth = 1;
      currentYear++;
    }
  }

  const resultado = results.flat();

  // Guardar JSON
  fs.writeFileSync('./jsonFiles/costoMO.json', JSON.stringify(resultado, null, 2), 'utf8');

  return resultado;
};

async function getCostoMOcontratista(ano,mes) {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;

    const url = `${baseUrl}/api/registroMOcontratista`;
  
  const resp = await axios.get(url, {
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    },
    params: {ano,mes}
  
  });

    fs.writeFileSync('./jsonFiles/costoMOcontratista.json', JSON.stringify(resp.data, null, 2), 'utf8');
  
    return resp.data;

};

async function getCostoMOcontratistaDesdeFecha(fechaDesde) {

  if (!fechaDesde) return [];

  // Parse seguro en horario local para evitar el desfase UTC (YYYY-MM-DD)
  let start;
  if (fechaDesde instanceof Date) {
    start = new Date(fechaDesde.getFullYear(), fechaDesde.getMonth(), fechaDesde.getDate());
  } else if (typeof fechaDesde === "string") {
    const m = fechaDesde.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) throw new Error("Fecha inválida en getCostoMOcontratistaDesdeFecha. Usa 'YYYY-MM-DD'.");
    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);
    start = new Date(y, mo - 1, d); // local
  } else {
    throw new Error("fechaDesde debe ser string 'YYYY-MM-DD' o Date.");
  }

  const today = new Date();

  let currentYear = start.getFullYear();
  let currentMonth = start.getMonth() + 1;

  const endYear = today.getFullYear();
  const endMonth = today.getMonth() + 1;

  const results = [];

  while (
    currentYear < endYear ||
    (currentYear === endYear && currentMonth <= endMonth)
  ) {

    console.log(`Procesando contratista ${currentYear}-${String(currentMonth).padStart(2, "0")}`);

    const res = await getCostoMOcontratista(currentYear, currentMonth);
    if (res) results.push(res);

    currentMonth++;
    if (currentMonth > 12) {
      currentMonth = 1;
      currentYear++;
    }
  }

  const resultado = results.flat();

  // Guardar JSON consolidado
  fs.writeFileSync('./jsonFiles/costoMOcontratista.json', JSON.stringify(resultado, null, 2), 'utf8');

  return resultado;
}

// endPoints Contables

async function getComprobantesContablesDocumentos(ano,mes) {

    const creds = await getConnectorCredentials();
    const {apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/comprobantesContablesDocumentos`;
  
  const resp = await axios.get(url, {
    params: { ano, mes }, 
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
  });
  
    console.log(resp.data);

};

async function getComprobanteContableDetalle(idComprobante) {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/comprobanteContableDetalle`;
  
  const resp = await axios.get(url, {
    params: { idComprobante}, 
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
    });

    console.log(resp.data);

};

async function getComprobantesContablesSoftland(ano,mes) {

    const creds = await getConnectorCredentials();
    const { apiCredentials } = creds;
    const { userName, userPassword, userToken } = apiCredentials;
    const url = `${baseUrl}/api/comprobantesContablesSoftland`;
  
  const resp = await axios.get(url, {
    params: { ano, mes}, 
    headers: {
  
        Authorization: `Bearer ${userToken}`,
      'x-user-name': userName,
      'x-user-password': userPassword
  
    }
  
    });

            fs.writeFile(`./jsonFiles/comprobantesSoftland.json`,JSON.stringify(resp.data),(err) => {

            if(err){
            
                throw new Error(err);
            
            }else{
            
                console.log('comprobantes Saved on JSON File');
            
            }

        }); 

    console.log(resp.data);

};

module.exports = {
    getEmpresas,
    getSucursales,
    getPredios,
    getCuarteles,
    getCentrosCosto,
    getCentrosCostoDistribucion,
 
    getEspecies,
    getVariedades,
    
    getFaenas,
    getLabores,
    
    getSuperFamilias,
    getFamilias,
    getSubFamilias,
    getItems,
    getUnidades,
    
    getPresupuestos,
    getGastosSinDistribuir,

    getTrabajadores,
    getTiposContrato,
    getCargos,
    getConceptos,
    getBonificaciones,
    getRegistroMO,
    getLiquidaciones,
    getCostoMO,
    getCostoMOdesdeFecha,
    getCostoMOcontratista,
    getCostoMOcontratistaDesdeFecha,

    getComprobantesContablesDocumentos,
    getComprobanteContableDetalle,
    getComprobantesContablesSoftland,
}