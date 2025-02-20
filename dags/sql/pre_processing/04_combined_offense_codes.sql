CREATE OR REPLACE TABLE combined.offense_codes
AS
select 
--ROW_NUMBER() OVER () as id,
COALESCE(lo.id, ro.id) id,
CASE 
    WHEN lo.Code IS NOT NULL THEN 'lincoln'
    ELSE 'rogers'
  END AS Source,
COALESCE(lo.Code, ro.Code) AS Code ,
COALESCE(lo.description, ro.description) AS description
from Chicago_PD_Data.src_Lincoln_Park_lincoln_Offense lo
full outer join
Chicago_PD_Data.src_Rogers_Park_offense ro
on lo.code=ro.code
order by id
