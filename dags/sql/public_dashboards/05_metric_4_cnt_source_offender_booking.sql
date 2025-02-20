CREATE OR REPLACE TABLE public_dashboard.metric_4_cnt_source_offender_booking
AS
select source,booking_date,count(distinct first_name||' '||last_name) as No_of_offenders_booked
from public_dashboard.all_offenses_base
group by 1,2
order by source,booking_date