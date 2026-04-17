--SQL query to find the consumer data with EFFECTIVE_DATE colum and check for invalid data in particular table:
--consumer tables with effective_date column
SELECT DISTINCT t.owner, t.table_name
FROM dba_tables t
JOIN dba_tab_columns c 
  ON t.owner = c.owner AND t.table_name = c.table_name
WHERE t.table_name LIKE 'CONSUMER%'
  AND c.column_name = 'EFFECTIVE_DATE'
ORDER BY t.owner, t.table_name;

--Check if invalid DST data exists in particular table
WITH years AS (
    SELECT EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), -12 * (LEVEL - 1))) AS yr
    FROM dual
    CONNECT BY LEVEL <= 50
),
dst AS (
    SELECT
        yr,
        CASE
            WHEN yr < 2007 THEN
                NEXT_DAY(TO_DATE(yr || '-04-01', 'YYYY-MM-DD') - 1, 'SUNDAY')
            ELSE
                NEXT_DAY(TO_DATE(yr || '-03-01', 'YYYY-MM-DD') - 1, 'SUNDAY') + 7
        END AS dst_start
    FROM years
)
SELECT *
FROM XXX a                                   --add a table name
JOIN dst d ON TRUNC(a.effective_date) = d.dst_start   
WHERE TO_CHAR(a.effective_date, 'HH24') = '02'         -- hour is between 2 and 2.59
      --AND a.record_expire_date > SYSDATE 
ORDER BY  a.effective_date DESC;
