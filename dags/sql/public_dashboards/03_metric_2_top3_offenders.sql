CREATE OR REPLACE TABLE public_dashboard.metric_2_top3_offenders
AS
select Upper(source) as Neighbourhood,first_name||' '||last_name as name,count(*) as No_of_offenses
from public_dashboard.all_offenses_base
group by 1,2
order by No_of_offenses desc