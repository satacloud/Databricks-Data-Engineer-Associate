----------------------------------------------------------------------------------------------------
--                               SDP Gold Table(s)                                                --
----------------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW chol_age_agg
AS
SELECT 
    HighCholest_Group, 
    Age_Group, 
    count(*) as Total
FROM health_silver
GROUP BY HighCholest_Group, Age_Group