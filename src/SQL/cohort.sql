/* 
add header
*/

-- create schema SX_DRIVERS;

-- global parameters
set schema_nm = 'OMOP_CDM.CDM';
set obs_tbl = $schema_nm || '.OBSERVATION';
set px_tbl = $schema_nm || '.PROCEDURE_OCCURRENCE';

-- source table validation preview
select * from identifier($obs_tbl) limit 5;
select * from identifier($px_tbl) where procedure_concept_id <> 0 or procedure_source_value is not null limit 5;


-- 
create or replace table all_delivery_cd (
    person_id varchar(50),
    visit_occurrence_id varchar(50),
    visit_detail_id varchar(50),
    event_date date,
    event_identifier varchar(20),
    event_source varchar(10)
)
;

insert into all_delivery_cd
select person_id,
       visit_occurrence_id, 
       visit_detail_id, 
       observation_date, 
       observation_concept_id,
       'observation'
from identifier($obs_tbl)
where observation_concept_id in (
    '765','766','767','768',
    '774','775',
    '783','784','785','786', '787','788',
    '796','797','798',
    '805','806','807'
)
;

insert into all_delivery_cd
select person_id,
       visit_occurrence_id, 
       visit_detail_id, 
       procedure_date, 
       procedure_concept_id,
       'procedure'
from identifier($px_tbl)
where procedure_source_value in (
    '59409','59514', '59612','59620',
    '10D00Z0','10D00Z1','10D00Z2','10D07Z3','10D07Z4', '10D07Z5', '10D07Z6','10D07Z7','10D07Z8',
    '10E0XZZ'
)
;

