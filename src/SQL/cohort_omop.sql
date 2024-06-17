/* 
add header
*/

-- create schema SX_DRIVERS;

-- global parameters
set schema_nm = 'OMOP_CDM.CDM';
set obs_tbl = $schema_nm || '.OBSERVATION';
set px_tbl = $schema_nm || '.PROCEDURE_OCCURRENCE';
set vis_tbl = $schema_nm || '.VISIT_OCCURRENCE';

-- source table validation preview
select * from identifier($obs_tbl) where observation_concept_id <> 0 limit 5;
select * from identifier($px_tbl) where procedure_concept_id <> 0 or procedure_source_value is not null limit 5;
select * from identifier($vis_tbl) limit 5;

-- collect all delivery codes
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
       'DRG'
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
       'CPT4'
from identifier($px_tbl)
where procedure_source_value in (
    '59409','59514', '59612','59620'
)
;
insert into all_delivery_cd
select person_id,
       visit_occurrence_id, 
       visit_detail_id, 
       procedure_date, 
       procedure_concept_id,
       'ICD10PCS'
from identifier($px_tbl)
where procedure_source_value in (
    '10D00Z0','10D00Z1','10D00Z2','10D07Z3','10D07Z4', '10D07Z5', '10D07Z6','10D07Z7','10D07Z8',
    '10E0XZZ'
)
;

-- identify independent delivery event
create or replace table ref_delivery_ip as 
select distinct
       a.person_id, 
       a.visit_occurrence_id,
       v.visit_start_date,
       v.visit_end_date
from all_delivery_cd a 
join identifier($vis_tbl) v 
on a.person_id = b.person_id and 
   a.visit_occurrence_id = v.visit_occurrence_id
where v.visit_concept_id in (
    9201, -- IP
    262   -- ER to IP
)
;
-- assume they don't overlap

create or replace table consolid_delivery as 
with cd_filter as (
    select v.person_id, 
           v.visit_occurrence_id,
           v.visit_start_date,
           v.visit_end_date,
           a.event_source,
           a.event_date,
           row_number() over (partition by v.person_id, v.visit_occurrence_id, a.event_source order by a.event_date) as rn_asc,
           row_number() over (partition by v.person_id, v.visit_occurrence_id, a.event_source order by a.event_date desc) as rn_desc
    from ref_delivery_ip v
    join all_delivery_cd a 
    on v.person_id = a.person_id and 
       v.visit_occurrence_id = a.visit_occurrence_id
    where a.event_date between dateadd('day',v.visit_start_date,-3) and dateadd('day',v.visit_end_date,3)
), f_pvt as (
    select * 
    from (
        select person_id, visit_occurrence_id,
               event_source, event_date
        from cd_filter
        where rn_asc = 1       
    )
    pivot (
        min(event_date) for event_source in (
            'DRG_DT','CPT4_DT','ICD10PCS_DT'
        )
    )
    as p(person_id,visit_occurrence_id,F_DRG_DT,F_CPT_DT,F_ICD_DT)
), l_pvt as (
    select * 
    from (
        select person_id, visit_occurrence_id,
               event_source, event_date
        from cd_filter
        where rn_desc = 1       
    )
    pivot (
        max(event_date) for event_source in (
            'DRG_DT','CPT4_DT','ICD10PCS_DT'
        )
    )
    as p(person_id,visit_occurrence_id,L_DRG_DT,L_CPT_DT,L_ICD_DT)
)
select a.person_id, 
       a.visit_occurrence_id,
       a.visit_start_date,
       a.visit_end_date,
       f.F_DRG_DT,f.F_CPT_DT,f.F_ICD_DT,
       l.L_DRG_DT,l.L_CPT_DT,l.L_ICD_DT
from ref_delivery_ip a 
left join f_pvt f on a.person_id = f.person_id and a.visit_occurrence_id = f.visit_occurrence_id
left join l_pvt l on a.person_id = l.person_id and a.visit_occurrence_id = l.visit_occurrence_id
;


create or replace table elig_delivery as 
with date_consolid as (
    select least() as event_start_dt,
           greatest() as event_end_dt
    
), 
select 