CREATE OR REPLACE TABLE combined.booking
AS
select 'lincoln' as source,* from boreal-graph-444300-j7.Chicago_PD_Data.src_Lincoln_Park_lincoln_Booking
union all

select 'rogers' as source,* from boreal-graph-444300-j7.Chicago_PD_Data.src_Rogers_Park_booking