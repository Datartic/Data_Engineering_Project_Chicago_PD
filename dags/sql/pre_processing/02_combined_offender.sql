CREATE OR REPLACE TABLE combined.offender
AS
select 'lincoln' as source,id,first_name,last_name,dob,race,sex,ethnicity,ssn 
from Chicago_PD_Data.src_Lincoln_Park_lincoln_Offender
union all
select 'rogers' as source,id,
CASE 
    WHEN ARRAY_LENGTH(SPLIT(name, ' ')) = 1 THEN 'FNU'
    WHEN ARRAY_LENGTH(SPLIT(name, ' ')) = 2 THEN SPLIT(name, ' ')[OFFSET(0)]
    ELSE ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(name, ' '), 0, ARRAY_LENGTH(SPLIT(name, ' ')) - 1), ' ')
  END AS first_name,
  
  CASE 
    WHEN ARRAY_LENGTH(SPLIT(name, ' ')) = 1 THEN name
    ELSE SPLIT(name, ' ')[OFFSET(ARRAY_LENGTH(SPLIT(name, ' ')) - 1)]
  END AS last_name,
  dob,race,sex,ethnicity,ssn 
  from Chicago_PD_Data.src_Rogers_Park_offender