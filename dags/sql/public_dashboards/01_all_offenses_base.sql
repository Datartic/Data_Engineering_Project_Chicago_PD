CREATE OR REPLACE TABLE public_dashboard.all_offenses_base
AS
select b.source,offf.id,offf.first_name,offf.last_name,offf.ssn,o.code,
o.description,CAST(b.booking_date as DATE) as booking_date,
offf.race,offf.ethnicity,offf.sex
from  combined.booking b
left join 
combined.booking_offense  bo
on b.id=bo.booking_id
and b.source = bo.source
left join combined.offense_codes o
on bo.offense_id=o.id
left join combined.offender as offf
on b.offender_id=offf.id
and b.source = offf.source