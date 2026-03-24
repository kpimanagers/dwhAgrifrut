const E_endPoints = require('./ETL/101E-endPoints');
const T_endPoints = require('./ETL/201T-endPoints');
const L_endPoints = require('./ETL/301L-endPoints');
const DM_endPoints = require('./ETL/302L-endpointsDM');
const O_endPoints = require('./ETL/401O-endPoints');
const cron = require('node-cron');

cron.schedule('0 1 * * *',()=>{

    (async()=>{

        const startTime = new Date();

        const fechaInicioTemporada = '2025-05-01'
        const idTemporada = 6

        //*** ----- INICIO CARGA ESTRUCTURAS ---- ***/
        //*** --- NO REQUIEREN TRANSFORMACIÓN --- ***/
        
        //*** ESTRUCTURA DE CENTROS DE COSTO

        const empresas = await E_endPoints.getEmpresas();
        await L_endPoints.updateBatchEmpresas(empresas);

        const sucursales = await E_endPoints.getSucursales();
        await L_endPoints.updateBatchSucursales(sucursales);

        const predios = await E_endPoints.getPredios();
        await L_endPoints.updateBatchPredios(predios);

        const cuarteles = await E_endPoints.getCuarteles();
        await L_endPoints.updateBatchCuarteles(cuarteles);

        const centrosCostos = await E_endPoints.getCentrosCosto();
        await L_endPoints.updateBatchCentrosCosto(centrosCostos);

        const centrosCostosDistribucion = await E_endPoints.getCentrosCostoDistribucion();
        await L_endPoints.updateBatchCentrosCostoDistribucion(centrosCostosDistribucion);

        //*** ESTRUCTURA DE ESPECIES

        const especies = await E_endPoints.getEspecies()
        await L_endPoints.updateBatchEspecies(especies);
  
        const variedades = await E_endPoints.getVariedades();
        await L_endPoints.updateBatchVariedades(variedades);

        //*** ESTRUCTURA DE LABORES

        const faenas = await E_endPoints.getFaenas();
        await L_endPoints.updateBatchFaenas(faenas);

        const labores = await E_endPoints.getLabores();
        await L_endPoints.updateBatchLabores(labores);

        //*** ESTRUCTURA DE ITEMS

        const superFamilias = await E_endPoints.getSuperFamilias();
        await L_endPoints.updateBatchItemsSuperFamilias(superFamilias);

        const familias = await E_endPoints.getFamilias();
        await L_endPoints.updateBatchItemsFamilias(familias);

        const subFamilias = await E_endPoints.getSubFamilias();
        await L_endPoints.updateBatchItemsSubFamilias(subFamilias);

        const items = await E_endPoints.getItems();
        await L_endPoints.updateBatchItems(items);

        const unidades = await E_endPoints.getUnidades();
        await L_endPoints.updateBatchUnidades(unidades);

        //**** --- FIN CARGA ESTRUCTURAS --- ****/

        //**** --- INICIO CARGA ESTRUCTURAS MANO DE OBRA --- ****/

        const trabajadores = await E_endPoints.getTrabajadores();
        await L_endPoints.updateBatchTrabajadores(trabajadores);
        
        const contratosTipo = await E_endPoints.getTiposContrato();
        await L_endPoints.updateBatchContratosTipo(contratosTipo);
        
        const cargos = await E_endPoints.getCargos();
        await L_endPoints.updateBatchCargos(cargos);

        const conceptos = await E_endPoints.getConceptos();
        await L_endPoints.updateBatchConceptos(conceptos);

        const bonificaciones = await E_endPoints.getBonificaciones();
        await L_endPoints.updateBatchBonificaciones(bonificaciones);

        //**** --- FIN CARGA ESTRUCTURAS MANO DE OBRA --- ****/


        //**** --- INICIO CARGA TRANSACCIONES --- ****/

        // Carga de Gestion
        const presupuesto = await E_endPoints.getPresupuestos();
        const presupuestoConverted = await T_endPoints.transformPresupuesto(presupuesto)
        await L_endPoints.insertBatchPresupuesto(presupuestoConverted);

        const gastosSinDistribuir = await E_endPoints.getGastosSinDistribuir(fechaInicioTemporada);
        const gastosSinDistribuirConverted = await T_endPoints.transformGastos(gastosSinDistribuir);
        await L_endPoints.insertBatchGastos(gastosSinDistribuirConverted,fechaInicioTemporada);

        //Carga Remuneraciones
        //const gastoMO = await E_endPoints.getCostoMO('2026','2');
        const gastoMO = await E_endPoints.getCostoMOdesdeFecha(fechaInicioTemporada);
        const gastoMOConverted = await T_endPoints.transformCostoMO(gastoMO);
        await L_endPoints.insertBatchCostoMODirecta(gastoMOConverted,fechaInicioTemporada);
        //const gastoMOContratista = await E_endPoints.getCostoMOcontratista('2026','1');
        const gastoMOContratista = await E_endPoints.getCostoMOcontratistaDesdeFecha(fechaInicioTemporada);
        const gastoMOContratistaConverted = await T_endPoints.transformCostoMOContratista(gastoMOContratista);
        await L_endPoints.insertBatchCostoMOcontratista(gastoMOContratistaConverted,fechaInicioTemporada)


        //**** --- FIN CARGA TRANSACCIONES --- ****/

        //**** --- INICIO CARGAS OPERACIONALES --- ****/

        const presupuestoDistribuido = await O_endPoints.distribuirPresupuesto(idTemporada); // Lo trae por id Temporada
        await L_endPoints.insertBatchPresupuestoDistribuido(presupuestoDistribuido,idTemporada);

        const gastosDistribuidos = await O_endPoints.distribuirGastos(idTemporada);
        await L_endPoints.insertBatchGastosDistribuidos(gastosDistribuidos,idTemporada);

        //**** --- FIN CARGAS OPERACIONALES --- ****/

        //**** --- INICIO CARGAS DATAMARTS --- ****/
        
        await DM_endPoints.insertGesDetalleGastos(idTemporada);
        
        //**** --- FIN CARGAS DATAMARTS --- ****/


        // Calcular tiempo total
        const endTime = new Date();
        const diffMs = endTime - startTime; // Diferencia en milisegundos

        // Convertir a HH:MM:SS
        const seconds = Math.floor(diffMs / 1000) % 60;
        const minutes = Math.floor(diffMs / (1000 * 60)) % 60;
        const hours = Math.floor(diffMs / (1000 * 60 * 60));

        // Formatear con dos dígitos
        const formattedHours = String(hours).padStart(2, '0');
        const formattedMinutes = String(minutes).padStart(2, '0');
        const formattedSeconds = String(seconds).padStart(2, '0');

        console.log(`Duración total del proceso: ${formattedHours}:${formattedMinutes}:${formattedSeconds}`);

 
    })();

});
