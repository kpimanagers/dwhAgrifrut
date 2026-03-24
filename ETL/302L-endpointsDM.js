const connFunc = require('../db/conexion');  

const sql = require('mssql');

async function insertGesDetalleGastos(intTemporadasId) {

    const conn = await connFunc.createConection();

    const insertSql = `
        insert into dmo.gesDetalleGastos (
            distribucion,
            tipo,
            fecha,
            ano,
            mes,
            cTemporada,
            nTemporada,
            rEmisor,
            tDocumento,
            numeroDocumento,
            rEmpresa,
            nEmpresa,
            cPredio,
            nPredio,
            cCuartel,
            nCuartel,
            cCentroCosto,
            nCentroCosto,
            hectareas,
            cEspecie,
            nEspecie,
            cVariedad,
            nVariedad,
            cFaena,
            nFaena,
            cLabor,
            nLabor,
            cFamilia,
            nFamilia,
            cSubFamilia,
            nSubFamilia,
            areaItem,
            cItem,
            nItem,
            cUnidad,
            cantidad,
            valorTotal
        )
select
    det.distribucion,
    det.tipo,
    det.fecha,
    year(det.fecha)  as ano,
    month(det.fecha) as mes,
    temp.intTemporadasId     as cTemporada,
    temp.intTemporadasNombre as nTemporada,
    det.rEmisor,
    det.tDocumento,
    det.numeroDocumento,
    emp.intEmpresasCodigo      as rEmpresa,
    emp.intEmpresasNombre  as nEmpresa,
    pr.intPrediosCodigo    as cPredio,
    pr.intPrediosNombre    as nPredio,
    cua.intCuartelesCodigo as cCuartel,
    cua.intCuartelesNombre as nCuartel,
    cc.intCentrosCostosCodigo as cCentroCosto,
    cc.intCentrosCostosNombre as nCentroCosto,
    tempc.hectareas,
    esp.intEspeciesCodigo  as cEspecie,
    esp.intEspeciesNombre  as nEspecie,
    var.intVariedadesCodigo as cVariedad,
    var.intVariedadesNombre as nVariedad,
    fae.intFaenasCodigo    as cFaena,
    fae.intFaenasNombre    as nFaena,
    lab.intLaboresCodigo   as cLabor,
    lab.intLaboresNombre   as nLabor,
    fam.intItemsFamiliasCodigo as cFamilia,
    fam.intItemsFamiliasNombre as nFamilia,
    sf.intItemsSubFamiliasCodigo as cSubFamilia,
    sf.intItemsSubFamiliasNombre as nSubFamilia,
    it.intItemsArea as areaItem,
    it.intItemsCodigo      as cItem,
    it.intItemsNombre      as nItem,
    un.intUnidadesCodigo   as cUnidad,
    det.cantidad,
    det.valorTotal
from (
        select
            'sinDistribuir'              as distribucion,
            'Real'                       as tipo,
            gto.fecha                    as fecha,
            gto.intEntidadesEmisoraCodigo      as rEmisor,
            gto.intDocumentosTipo        as tDocumento,
            gto.numeroDocumento          as numeroDocumento,
            gto.intCentrosCostosCodigo       as intCentrosCostosCodigo,
            gto.intItemsCodigo               as intItemsCodigo,
            gto.intLaboresCodigo             as intLaboresCodigo,
            cast(gto.cantidad   as decimal(18,4)) as cantidad,
            cast(gto.valorTotal as decimal(18,2)) as valorTotal
        from gesGastos as gto

        union all

        select
            'sinDistribuir'              as distribucion,
            'PPTO'                       as tipo,
            pto.fecha                    as fecha,
            null                         as rEmisor,
            null                         as tDocumento,
            null                         as numeroDocumento,
            pto.intCentrosCostosCodigo       as intCentrosCostosCodigo,
            pto.intItemsFamiliasCodigo       as intItemsCodigo,
            pto.intLaboresCodigo             as intLaboresCodigo,
            cast(pto.cantidad as decimal(18,4)) as cantidad,
            cast(pto.valorTotal    as decimal(18,2)) as valorTotal
        from gesPresupuestos as pto

        union all

        select
            'Distribuido'                as distribucion,
            'Real'                       as tipo,
            gto.fecha                    as fecha,
            gto.intEntidadesEmisoraCodigo      as rEmisor,
            gto.intDocumentosTipo        as tDocumento,
            gto.numeroDocumento          as numeroDocumento,
            gto.intCentrosCostosCodigo       as intCentrosCostosCodigo,
            gto.intItemsCodigo               as intItemsCodigo,
            gto.intLaboresCodigo             as intLaboresCodigo,
            cast(gto.cantidad   as decimal(18,4)) as cantidad,
            cast(gto.valorTotal as decimal(18,2)) as valorTotal
        from gesGastosDistribuidos as gto

        union all

        select
            'Distribuido'                as distribucion,
            'PPTO'                       as tipo,
            pto.fecha                    as fecha,
            null                         as rEmisor,
            null                         as tDocumento,
            null                         as numeroDocumento,
            pto.intCentrosCostosCodigo       as intCentrosCostosCodigo,
            pto.intItemsFamiliasCodigo       as intItemsCodigo,
            pto.intLaboresCodigo             as intLaboresCodigo,
            cast(pto.cantidad as decimal(18,4)) as cantidad,
            cast(pto.valorTotal    as decimal(18,2)) as valorTotal
        from gesPresupuestosDistribuidos as pto
    ) as det
    left join intCentrosCostos as cc on det.intCentrosCostosCodigo = cc.intCentrosCostosCodigo
    left join intCuarteles as cua on cc.intCuartelesCodigo = cua.intCuartelesCodigo
    left join intPredios as pr on cc.intPrediosCodigo = pr.intPrediosCodigo
    left join intEmpresas as emp on cc.intEmpresasCodigo = emp.intEmpresasCodigo
    left join intVariedades as var on cc.intVariedadesCodigo = var.intVariedadesCodigo
    left join intEspecies as esp on var.intEspeciesCodigo = esp.intEspeciesCodigo
    left join intItems as it on det.intItemsCodigo = it.intItemsCodigo
    left join intItemsSubFamilias as sf on it.intItemsSubFamiliasCodigo = sf.intItemsSubFamiliasCodigo
    left join intItemsFamilias as fam on sf.intItemsFamiliasCodigo = fam.intItemsFamiliasCodigo
    left join intUnidades as un on it.intUnidadesCodigo = un.intUnidadesCodigo
    left join intLabores as lab on det.intLaboresCodigo = lab.intLaboresCodigo
    left join intFaenas as fae on lab.intFaenasCodigo = fae.intFaenasCodigo
    left join intTemporadas as temp on det.fecha between temp.fechaInicio and temp.fechaTermino
    left join intTemporadasConfiguracion as tempc
        on temp.intTemporadasId = tempc.intTemporadasId
        and tempc.intCentrosCostosCodigo = cc.intCentrosCostosCodigo
    where temp.intTemporadasId = @cTemporada
    `;

    const transaction = new sql.Transaction(conn);

    try {

        await transaction.begin();

        // opcional pero recomendado: validar parámetro antes de ejecutar
        if (intTemporadasId === null || intTemporadasId === undefined || intTemporadasId === '') {
            throw new Error('intTemporadasId viene vacío');
        }

        const reqDelete = new sql.Request(transaction);
        reqDelete.input('cTemporada', sql.Int, intTemporadasId);
        await reqDelete.query(`delete from dmo.gesDetalleGastos where cTemporada = @cTemporada`);

        const reqInsert = new sql.Request(transaction);
        reqInsert.input('cTemporada', sql.Int, intTemporadasId);
        await reqInsert.query(insertSql);

        await transaction.commit();

        console.log('insertGesDetalleGastos completed');
        return { ok: true };

    } catch (err) {

        try { await transaction.rollback(); } catch (_) {}
        throw err;

    } finally {

        try { await conn.close(); } catch (_) {}

    }
}

async function insertDetalleGastos() {

    const conn = await connFunc.createConection();

    const insertSql = `
        insert into dmo.detalleGastos (
            distribucion,
            tempOrder,
            ano,
            mes,
            cTemporada,
            nTemporada,
            rEmpresa,
            nEmpresa,
            nEspecie,
            nFamilia,
            nSubFamilia,
            nFaena,
            cCentroCosto,
            nCentroCosto,
            gastoReal,
            presupuesto
        )
        select
            distribucion,
            concat(ano, '-', right('00' + cast(mes as varchar(2)), 2)) as tempOrder,
            ano,
            mes,
            cTemporada,
            nTemporada,
            rEmpresa,
            nEmpresa,
            isnull(nEspecie,'SIN ESPECIE'),
            isnull(nFamilia,'SIN FAMILIA'),
            isnull(nSubFamilia,'SIN SUB FAMILIA'),
            isnull(nFaena,'SIN FAENA'),
            cCentroCosto,
            nCentroCosto,
            cast(sum(case when tipo = 'Real' then valorTotal else 0 end) as decimal(18,6)) as gastoReal,
            cast(sum(case when tipo = 'PPTO' then valorTotal else 0 end) as decimal(18,6)) as presupuesto
        from dmo.gesDetalleGastos
        where tipo in ('Real','PPTO')
        group by
            distribucion,
            concat(ano, '-', right('00' + cast(mes as varchar(2)), 2)),
            ano,
            mes,
            cTemporada,
            nTemporada,
            rEmpresa,
            nEmpresa,
            isnull(nEspecie,'SIN ESPECIE'),
            isnull(nFamilia,'SIN FAMILIA'),
            isnull(nSubFamilia,'SIN SUB FAMILIA'),
            isnull(nFaena,'SIN FAENA'),
            cCentroCosto,
            nCentroCosto
    `;

    const transaction = new sql.Transaction(conn);

    try {

        await transaction.begin();

        await new sql.Request(transaction).query(`
            delete from dmo.detalleGastos
        `);

        await new sql.Request(transaction).query(insertSql);

        await transaction.commit();

        console.log('insertDetalleGastos completed');
        return { ok: true };

    } catch (err) {

        try { await transaction.rollback(); } catch (_) {}
        throw err;

    } finally {

        try { await conn.close(); } catch (_) {}

    }
}

async function insertDirectoVSDistribuido() {

    const conn = await connFunc.createConection();

    const insertSql = `
        insert into dmo.directoVSdistribuido (
            distribucion,
            tipo,
            ano,
            mes,
            nTemporada,
            cTemporada,
            rEmpresa,
            nEmpresa,
            nEspecie,
            valorSinDistribuir,
            valorDistribucion,
            valorDistribuido
        )
        select
            'Resumen' as distribucion,
            tipo,
            ano,
            mes,
            nTemporada,
            cTemporada,
            rEmpresa,
            nEmpresa,
            isnull(nEspecie,'SIN ESPECIE') as nEspecie,
            cast(sum(case when distribucion = 'SinDistribuir' then valorTotal else 0 end) as decimal(18,6)),
            cast(
                sum(case when distribucion = 'Distribuido' then valorTotal else 0 end)
              - sum(case when distribucion = 'SinDistribuir' then valorTotal else 0 end)
            as decimal(18,6)),
            cast(sum(case when distribucion = 'Distribuido' then valorTotal else 0 end) as decimal(18,6))
        from dmo.gesDetalleGastos
        where tipo in ('Real','PPTO')
          and distribucion in ('Distribuido','SinDistribuir')
        group by
            ano,
            mes,
            nTemporada,
            cTemporada,
            rEmpresa,
            nEmpresa,
            isnull(nEspecie,'SIN ESPECIE'),
            tipo
    `;

    const transaction = new sql.Transaction(conn);

    try {

        await transaction.begin();

        await new sql.Request(transaction).query(`
            delete from dmo.directoVSdistribuido
        `);

        await new sql.Request(transaction).query(insertSql);

        await transaction.commit();

        console.log('insertDirectoVSDistribuido completed');
        return { ok: true };

    } catch (err) {

        try { await transaction.rollback(); } catch (_) {}
        throw err;

    } finally {

        try { await conn.close(); } catch (_) {}

    }
}

async function insertAvanceXdimension() {

    const conn = await connFunc.createConection();

    const insertSql = `
        insert into dmo.avanceXdimension (
            nEmpresa,
            nTemporada,
            ano,
            mes,
            dimension,
            valorDimension,
            valorPpto,
            valorGasto,
            avance
        )
        select
            nEmpresa,
            nTemporada,
            ano,
            mes,
            dimension,
            valorDimension,
            cast(valorPpto as decimal(18,6)),
            cast(valorGasto as decimal(18,6)),
            cast(avance as decimal(18,6))
        from (

            select
              nEmpresa,
              nTemporada,
              ano,
              mes,
              'especie' as dimension,
              isnull(nEspecie,'SIN ESPECIE') as valorDimension,
              sum(case when tipo = 'ppto' then valorTotal else 0 end) as valorPpto,
              sum(case when tipo = 'real' then valorTotal else 0 end) as valorGasto,
              case
                when sum(case when tipo = 'ppto' then valorTotal else 0 end) = 0 then 0
                else
                  sum(case when tipo = 'real' then valorTotal else 0 end) * 1.0 /
                  sum(case when tipo = 'ppto' then valorTotal else 0 end)
              end as avance
            from dmo.gesDetalleGastos
            where distribucion = 'sindistribuir'
            group by
              nEmpresa,
              nTemporada,
              ano,
              mes,
              isnull(nEspecie,'SIN ESPECIE')

            union all

            select
              nEmpresa,
              nTemporada,
              ano,
              mes,
              'faena' as dimension,
              isnull(nFaena,'SIN FAENA') as valorDimension,
              sum(case when tipo = 'ppto' then valorTotal else 0 end),
              sum(case when tipo = 'real' then valorTotal else 0 end),
              case
                when sum(case when tipo = 'ppto' then valorTotal else 0 end) = 0 then 0
                else
                  sum(case when tipo = 'real' then valorTotal else 0 end) * 1.0 /
                  sum(case when tipo = 'ppto' then valorTotal else 0 end)
              end
            from dmo.gesDetalleGastos
            where distribucion = 'sindistribuir'
            group by
              nEmpresa,
              nTemporada,
              ano,
              mes,
              isnull(nFaena,'SIN FAENA')

            union all

            select
              nEmpresa,
              nTemporada,
              ano,
              mes,
              'familiaItem' as dimension,
              isnull(nFamilia,'SIN FAMILIA') as valorDimension,
              sum(case when tipo = 'ppto' then valorTotal else 0 end),
              sum(case when tipo = 'real' then valorTotal else 0 end),
              case
                when sum(case when tipo = 'ppto' then valorTotal else 0 end) = 0 then 0
                else
                  sum(case when tipo = 'real' then valorTotal else 0 end) * 1.0 /
                  sum(case when tipo = 'ppto' then valorTotal else 0 end)
              end
            from dmo.gesDetalleGastos
            where distribucion = 'sindistribuir'
            group by
              nEmpresa,
              nTemporada,
              ano,
              mes,
              isnull(nFamilia,'SIN FAMILIA')

        ) t
    `;

    const transaction = new sql.Transaction(conn);

    try {

        await transaction.begin();

        await new sql.Request(transaction).query(`
            delete from dmo.avanceXdimension
        `);

        await new sql.Request(transaction).query(insertSql);

        await transaction.commit();

        console.log('insertAvanceXdimension completed');
        return { ok: true };

    } catch (err) {

        try { await transaction.rollback(); } catch (_) {}
        throw err;

    } finally {

        try { await conn.close(); } catch (_) {}

    }
}

async function insertAvanceXespecie() {

    const conn = await connFunc.createConection();

    const insertSql = `
        insert into dmo.avanceXespecie (
            nEmpresa,
            nTemporada,
            nEspecie,
            valorPpto,
            valorGasto,
            avance,
            totalPpto_empresa,
            totalGasto_empresa
        )
        select
          nEmpresa,
          nTemporada,
          nEspecie,
          cast(valorPpto as decimal(18,6)),
          cast(valorGasto as decimal(18,6)),
          cast(avance as decimal(18,6)),
          cast(totalPpto_empresa as decimal(18,6)),
          cast(totalGasto_empresa as decimal(18,6))
        from (
            select
              nEmpresa,
              nTemporada,
              case when nEspecie is null then 'SIN ESPECIE' else nEspecie end as nEspecie,

              sum(case when tipo = 'ppto' then valorTotal else 0 end) as valorPpto,
              sum(case when tipo = 'real' then valorTotal else 0 end) as valorGasto,

              case
                when sum(case when tipo = 'ppto' then valorTotal else 0 end) = 0 then 0
                else
                  sum(case when tipo = 'real' then valorTotal else 0 end) * 1.0 /
                  sum(case when tipo = 'ppto' then valorTotal else 0 end)
              end as avance,

              sum(sum(case when tipo = 'ppto' then valorTotal else 0 end))
                over (partition by nEmpresa, nTemporada) as totalPpto_empresa,

              sum(sum(case when tipo = 'real' then valorTotal else 0 end))
                over (partition by nEmpresa, nTemporada) as totalGasto_empresa

            from dmo.gesDetalleGastos
            where distribucion = 'sindistribuir'
            group by
              nEmpresa,
              nTemporada,
              case when nEspecie is null then 'SIN ESPECIE' else nEspecie end
        ) t
    `;

    const transaction = new sql.Transaction(conn);

    try {

        await transaction.begin();

        await new sql.Request(transaction).query(`
            delete from dmo.avanceXespecie
        `);

        await new sql.Request(transaction).query(insertSql);

        await transaction.commit();

        console.log('insertAvanceXespecie completed');
        return { ok: true };

    } catch (err) {

        try { await transaction.rollback(); } catch (_) {}
        throw err;

    } finally {

        try { await conn.close(); } catch (_) {}

    }
}

async function insertGlobalSeasonValues() {

    const conn = await connFunc.createConection();

    const insertSql = `
        insert into dmo.globalSeasonValues (
            nEmpresa,
            nTemporada,
            valorPpto,
            valorGasto,
            avance
        )
        select
          nEmpresa,
          nTemporada,

          cast(sum(case 
                when tipo = 'ppto' 
                 and distribucion = 'sindistribuir' 
                then valorTotal 
                else 0 
              end) as decimal(18,6)) as valorPpto,

          cast(sum(case 
                when tipo = 'real' 
                 and distribucion = 'sindistribuir' 
                then valorTotal 
                else 0 
              end) as decimal(18,6)) as valorGasto,

          cast(case 
            when sum(case 
                       when tipo = 'ppto' 
                        and distribucion = 'sindistribuir' 
                       then valorTotal 
                       else 0 
                     end) = 0
            then 0
            else
              sum(case 
                    when tipo = 'real' 
                     and distribucion = 'sindistribuir' 
                    then valorTotal 
                    else 0 
                  end) * 1.0 /
              sum(case 
                    when tipo = 'ppto' 
                     and distribucion = 'sindistribuir' 
                    then valorTotal 
                    else 0 
                  end)
          end as decimal(18,6)) as avance

        from dmo.gesDetalleGastos
        group by
          nEmpresa,
          nTemporada
    `;

    const transaction = new sql.Transaction(conn);

    try {

        await transaction.begin();

        await new sql.Request(transaction).query(`
            delete from dmo.globalSeasonValues
        `);

        await new sql.Request(transaction).query(insertSql);

        await transaction.commit();

        console.log('insertGlobalSeasonValues completed');
        return { ok: true };

    } catch (err) {

        try { await transaction.rollback(); } catch (_) {}
        throw err;

    } finally {

        try { await conn.close(); } catch (_) {}

    }
}

module.exports = {

    insertGesDetalleGastos,
    insertDetalleGastos,
    insertDirectoVSDistribuido,
    insertAvanceXdimension,
    insertAvanceXespecie,
    insertGlobalSeasonValues

}
