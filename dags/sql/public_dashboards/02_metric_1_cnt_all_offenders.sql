CREATE OR REPLACE TABLE public_dashboard.metric_1_cnt_all_offenders
AS
select Upper(source) as Neighbourhood,count(distinct first_name||last_name) Total_Offenders
from public_dashboard.all_offenses_base
group by 1