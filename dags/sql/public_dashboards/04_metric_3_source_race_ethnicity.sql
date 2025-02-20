CREATE OR REPLACE TABLE public_dashboard.metric_3_source_race_ethnicity_sex
AS
with final as (
select distinct Upper(source) as Neighbourhood ,first_name||' '||last_name , race, ethnicity,sex
from public_dashboard.all_offenses_base
) 
select final.Neighbourhood,race,ethnicity,sex,count(race) as Cnt_Race,
count(ethnicity)  as Cnt_Ethnicity ,count(case when sex='Male' then 1 else null end)  as Cnt_Male,
count(case when sex='Female' then 1 else null end)  as Cnt_Female
from final
group by 1,2,3,4
