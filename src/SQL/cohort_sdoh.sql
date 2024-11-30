/* 
add header

inst: ID
role: GROUSE_ROLE_B_ADMIN
db: GROUSE_ANALYTICS_DB
schema: SX_DRIVERS
wh: GROUSE_MEDIUM_WH
*/


select * from SDOH_DB.ACXIOM.MU_GEOID_DEID limit 5;
select * from SX_SDOH.S_SDH_SEL;
select * from DRIVERSPATS limit 5;


create or replace table MATERNAL_COHORT as 
select a.patid, b.census_block_group_2020
from DRIVERSPATS a 
join SDOH_DB.ACXIOM.MU_GEOID_DEID b 
on a.patid = b.patid
;

select count(distinct patid), count(distinct census_block_group_2020) 
from maternal_cohort
;
-- 15092	1378

create or replace procedure get_sdoh_s(
    TGT_TABLE string,
    REF_COHORT string,
    REF_PKEY string,
    REF_COVAR string,
    SDOH_TBLS array,
    DRY_RUN boolean,
    DRY_RUN_AT string
)
returns variant
language javascript
as
$$
/**
 * @param {string} TGT_TABLE: name of target sdoh collection table
 * @param {string} REF_COHORT: name of reference patient table (absolute path/full name), should at least include (patid)
 * @param {string} REF_PKEY: primary key column in REF_COHORT table for matchin with SDOH tables
 * @param {string} REF_COVAR: name of reference covariate selection table
 * @param {array} SDOH_TBLS: an array of tables in SDOH_DB
 * @param {boolean} DRY_RUN: dry run indicator. If true, only sql script will be created and stored in dev.sp_out table
 * @param {boolean} DRY_RUN_AT: A temporary location to store the generated sql query for debugging purpose. 
                                When DRY_RUN = True, provide absolute path (full name) to the table; when DRY_RUN = False, provide NULL 
**/
if (DRY_RUN) {
    var log_stmt = snowflake.createStatement({
        sqlText: `CREATE OR REPLACE TEMPORARY TABLE `+ DRY_RUN_AT +`(QRY VARCHAR);`});
    log_stmt.execute(); 
}

// collect all sdoh tables and their columns
var sdoh_tbl_quote = SDOH_TBLS.map(item => `'${item}'`)
var get_tbl_cols_qry = `
    SELECT a.table_schema, a.table_name, listagg(a.column_name,',') AS enc_col, b.var_type
    FROM SDOH_DB.information_schema.columns a
    JOIN `+ REF_COVAR +` b
    ON a.table_schema = b.var_domain and 
       a.table_name = b.var_subdomain and 
       a.column_name = b.var
    WHERE a.table_name in (`+ sdoh_tbl_quote +`) 
    GROUP BY a.table_schema, a.table_name, b.var_type;`
var get_tbl_cols = snowflake.createStatement({sqlText: get_tbl_cols_qry});
var tables = get_tbl_cols.execute();

// loop over listed tables
while (tables.next()){
    var schema = tables.getColumnValue(1);
    var table = tables.getColumnValue(2);
    var cols = tables.getColumnValue(3).split(",");
    var cols_alias = cols.map(value => {return 'b.'+ value});
    var type = tables.getColumnValue(4);

    // keep records in original categorical format
    var sqlstmt = `
        insert into `+ TGT_TABLE +`(PATID,GEOCODEID,GEO_ACCURACY,SDOH_VAR,SDOH_VAL,SDOH_TYPE,SDOH_SRC)
        select  PATID,
                GEOCODEID,
                GEO_ACCURACY,
                SDOH_VAR,
                SDOH_VAL,
                '`+ type +`' as SDOH_TYPE,
                '`+ schema +`' as SDOH_SRC
        from (
            select  a.patid, 
                    b.geocodeid,
                    b.geo_accuracy,
                    `+ cols_alias +`
            from `+ REF_COHORT +` a 
            join SDOH_DB.`+ schema +`.`+ table +` b 
            on startswith(a.`+ REF_PKEY +`,b.geocodeid)
            -- on substr(a.CENSUS_BLOCK_GROUP_2020,1,length(b.geocodeid)) = b.geocodeid
            where length(b.geocodeid) > 9 -- excluding zip, fips-st, fips-cty
        )
        unpivot 
        (
            SDOH_VAL for SDOH_VAR in (`+ cols +`)
        )
        where SDOH_VAL is not null
    `;

    var run_sqlstmt = snowflake.createStatement({sqlText: sqlstmt});

    if (DRY_RUN) {
        // preview of the generated dynamic SQL scripts - comment it out when perform actual execution
        var log_stmt = snowflake.createStatement({
                    sqlText: `INSERT INTO `+ DRY_RUN_AT +` (qry) values (:1);`,
                    binds: [sqlstmt]});
        log_stmt.execute(); 
    } else {
        // run dynamic dml query
        var commit_txn = snowflake.createStatement({sqlText: `commit;`}); 
        try{run_sqlstmt.execute();} catch(error) {};
        commit_txn.execute();
    }
}
$$
;

create or replace table MATERNAL_SSDH (
        PATID varchar(50) NOT NULL
       ,GEOCODEID varchar(15)
       ,GEO_ACCURACY varchar(3)
       ,SDOH_VAR varchar(100)
       ,SDOH_VAL varchar(1000)
       ,SDOH_TYPE varchar(2)
       ,SDOH_SRC varchar(10)
);

/* test */
-- call get_sdoh_s(
--        'MATERNAL_SSDH',
--        'MATERNAL_COHORT',
--        'CENSUS_BLOCK_GROUP_2020',
--        'SX_SDOH.S_SDH_SEL',
--        array_construct(
--               'RUCA_TR_2010'
--              ,'SVI_TR_2020'
--        ),
--        True, 'TMP_SP_OUTPUT'
-- );
-- select * from TMP_SP_OUTPUT;

call get_sdoh_s(
       'MATERNAL_SSDH',
       'MATERNAL_COHORT',
       'CENSUS_BLOCK_GROUP_2020',
       'SX_SDOH.S_SDH_SEL',
       array_construct(
              'ACS_TR_2015_2019'
             ,'ADI_BG_2020'
             ,'RUCA_TR_2010'
             ,'SVI_TR_2020'
             ,'FARA_TR_2019'
             ,'MUA_X_2024'
             ,'MUP_X_2024'
             ,'SLD_BG_2021'
       ),
       FALSE, NULL
);

select * from MATERNAL_SSDH limit 5;

select SDOH_SRC, count(*) 
from MATERNAL_SSDH 
group by SDOH_SRC;

create or replace temporary view var_lst as 
select distinct sdoh_src, sdoh_var
from MATERNAL_SSDH
order by sdoh_src, sdoh_var
;

select 'select * from pivot_test pivot (median(sdoh_val) for sdoh_var in ('||listagg (distinct ''''||sdoh_var||'''' , ',') ||')) as p(PATID,'||listagg (distinct sdoh_var,',') ||') order by patid;' from var_lst;

create or replace table MATERNAL_SSDH_DEID as 
with cnt_field as (
    select a.*, 
           count(distinct a.sdoh_var) over (partition by a.patid, a.geocodeid) as sdh_avail_cnt
    from MATERNAL_SSDH a
    where regexp_like(sdoh_val, '^[0-9]+$') -- numerical only
),   dedup as (
    select b.*, 
           row_number() over (partition by b.patid,b.sdoh_var order by b.sdh_avail_cnt desc) rn
    from cnt_field b
    order by sdoh_src, sdoh_var
)
select * from 
(select patid, sdoh_var, try_to_numeric(sdoh_val) as sdoh_val from dedup where rn = 1)
pivot (
    median(sdoh_val) for sdoh_var in (
        'LA1AND20','LAHALFAND10','LATRACTS1','AC_WATER','CBSA_WRK','D1A','D1B','D1C5_RET',
        'D1C8_HLTH','D1C8_OFF','D2B_E5MIX','D2B_RANKED','D3AMM','D5BE','D5CEI','D5CRI',
        'PCT_AO2P','CBSA_EMP','D1C8_IND','D1C8_RET','D3AAO','D3BPO3','D5CR','E8_ED',
        'D2R_JOBPOP','D1C5_ENT','D5DEI','E5_OFF','E5_SVC','E8_ENT','HH','E8_SVC','CRANE_17',
        'CRANE_18','CRANE_39','CRANE_43','CRANE_56','CRANE_31','CRANE_35','CRANE_42',
        'CRANE_51','CRANE_52','POVERTYRATE','LAKIDS1SHARE','LALOWIHALFSHARE','LANHOPIHALFSHARE',
        'LAOMULTIR1SHARE','LAPOP1SHARE','LAASIAN10SHARE','LABLACK10SHARE','LALOWI10SHARE',
        'LAPOP10SHARE','LASNAP10SHARE','LAPOP20SHARE','LASENIORS1SHARE','LAHUNV10SHARE',
        'EP_AGE17','EP_HISP','EP_LIMENG','EP_MUNIT','EP_OTHERRACE','EPL_DISABL','EPL_MOBILE',
        'RPL_THEME2','EP_GROUPQ','EPL_UNEMP','F_THEME4','F_MINRTY','F_UNEMP','RUCA_SECONDARY',
        'AC_UNPR','AUTOOWN1','AUTOOWN2P','D1C','D1C8_ENT','D1C8_PUB','D2A_WRKEMP','D2C_TRPMX2',
        'D4B050','D5AR','D1C8_ED','D2C_TRPMX1','D3A','D3B_RANKED','D3BPO4','D4E','E_HIWAGEWK',
        'WORKERS','D3BAO','D1_FLAG','ADI_STATERANK','D1C5_OFF','D5DR','E5_ENT','NATWALKIND','E8_IND',
        'ADI_NATRANK','BIRD_INDEX','CRANE_11','CRANE_12','CRANE_19','CRANE_22','CRANE_26','CRANE_28',
        'CRANE_30','CRANE_32','CRANE_36','CRANE_4','CRANE_44','CRANE_46','CRANE_47','CRANE_15',
        'CRANE_25','CRANE_33','CRANE_45','CRANE_55','CRANE_40','CRANE_6','MEDIANFAMILYINCOME',
        'LABLACK1SHARE','LAHUNV1SHARE','LAPOPHALFSHARE','LASENIORSHALFSHARE','LASNAPHALFSHARE',
        'LAOMULTIR10SHARE','LAHUNV20SHARE','LALOWI20SHARE','LAOMULTIR20SHARE','CRANE_34','CRANE_5',
        'CRANE_53','CRANE_7','LAASIAN1SHARE','LAWHITEHALFSHARE','LANHOPI10SHARE','LAASIAN20SHARE',
        'INDEX_OF_MEDICAL_UNDERSERVICE_SCORE','EP_ASIAN','EPL_CROWD','EPL_SNGPNT','RPL_THEME1',
        'RPL_THEMES','EPL_AGE65','EPL_LIMENG','EPL_NOHSDP','EP_NOHSDP','F_AGE65','F_DISABL',
        'F_LIMENG','F_MUNIT','F_NOVEH','GROUPQUARTERSFLAG','HUNVFLAG','LATRACTS10','LILATRACTS_1AND10',
        'LILATRACTS_1AND20','LILATRACTS_HALFAND10','LOWINCOMETRACTS','AC_LAND','AUTOOWN0','COUNTHU',
        'D2B_E8MIX','D2B_E8MIXA','D3BMM3','D3BMM4','D4C','D5AE','E_MEDWAGEWK','D2B_E5MIXA','D2C_TRIPEQ',
        'D3B','E_PCTLOWWAGE','E8_HLTH','D5DRI','LATRACTSVEHICLE_20','D4A','E8_PUB','E8_RET','E5_IND',
        'CRANE_10','CRANE_24','CRANE_37','CRANE_9','CRANE_48','OHU2010','LAAIAN1SHARE','LABLACKHALFSHARE',
        'LAOMULTIRHALFSHARE','LASNAP1SHARE','LAWHITE1SHARE','LAKIDS10SHARE','LAAIAN20SHARE','LAKIDS20SHARE',
        'LASNAP20SHARE','LANHOPI1SHARE','LAAIAN10SHARE','LAHISP20SHARE','EP_AFAM','EP_AGE65','EP_HBURD',
        'EP_NOINT','EP_UNEMP','EPL_HBURD','EPL_MINRTY','EPL_NOVEH','F_THEME3','F_TOTAL','RPL_THEME3',
        'EP_MINRTY','EP_MOBILE','EP_TWOMORE','EPL_MUNIT','EPL_POV150','F_THEME2','F_AGE17','F_CROWD',
        'F_HBURD','F_POV150','F_SNGPNT','F_GROUPQ','F_NOHSDP','RUCA_PRIMARY','LA1AND10','LATRACTS_HALF',
        'LATRACTS20','LILATRACTS_VEHICLE','URBAN','AC_TOTAL','D1C5_IND','D1C5_SVC','D2A_RANKED','D2R_WRKEMP',
        'D4D','D5DE','E_LOWWAGEWK','E8_OFF','D1D','D5BR','R_PCTLOWWAGE','D4B025','D2A_EPHHM','D2A_JPHH',
        'PCT_AO0','D3APO','E5_RET','D5CE','D4A_RANKED','PCT_AO1','P_WRKAGE','D1C8_SVC','CRANE_1','CRANE_13',
        'CRANE_14','CRANE_16','CRANE_20','CRANE_21','CRANE_27','CRANE_2','CRANE_23','CRANE_29','CRANE_38',
        'CRANE_41','CRANE_50','CRANE_54','CRANE_8','LAHISP1SHARE','LAHUNVHALFSHARE','LALOWI1SHARE','LAHISP10SHARE',
        'LASENIORS10SHARE','LAWHITE20SHARE','LAKIDSHALFSHARE','CRANE_49','CRANE_3','LAASIANHALFSHARE','LAHISPHALFSHARE',
        'LAWHITE10SHARE','LABLACK20SHARE','LANHOPI20SHARE','LASENIORS20SHARE','EP_AIAN','EP_CROWD','EP_DISABL',
        'EP_NOVEH','EP_POV150','EP_SNGPNT','EP_UNINSUR','EPL_AGE17','EPL_GROUPQ','EPL_UNINSUR','F_THEME1',
        'EP_NHPI','F_MOBILE','F_UNINSUR','RPL_THEME4'
        )
)
order by patid;
;

select * from MATERNAL_SSDH_DEID limit 5;

select count(distinct patid), count(*) from MATERNAL_SSDH_DEID;

create schema SDOH_DB.SSDH;
create or replace table SDOH_DB.SSDH.MATERNAL_SSDH_DEID as
select * from GROUSE_ANALYTICS_DB.SX_DRIVERS.MATERNAL_SSDH_DEID;


select * from SDOH_DB.SSDH.MATERNAL_SSDH_DEID limit 10;
select "'ADI_NATRANK'" from SDOH_DB.SSDH.MATERNAL_SSDH_DEID;
