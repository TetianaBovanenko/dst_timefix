--Query to update tables of SCD Type 2 change (the ones which have column RECORD_EXPIRE_DATE):


--Step 1. Identify all the tables with invalid DST (within the current environment):

DROP TABLE temp_invalid_dst_tables;

CREATE GLOBAL TEMPORARY TABLE temp_invalid_dst_tables (
    table_owner VARCHAR2(128),
    table_name  VARCHAR2(128),
    row_count   NUMBER
) ON COMMIT PRESERVE ROWS;


BEGIN
    DELETE FROM temp_invalid_dst_tables;   -- clear previous results
    
    FOR t IN (SELECT owner, table_name 
              FROM all_tables 
              WHERE table_name LIKE 'CONSUMER%'
              AND EXISTS (SELECT 1 FROM all_tab_columns 
                          WHERE owner = all_tables.owner 
                          AND table_name = all_tables.table_name 
                          AND column_name = 'EFFECTIVE_DATE'))
    LOOP
        EXECUTE IMMEDIATE '
            DECLARE
                cnt NUMBER;
            BEGIN
                SELECT COUNT(*) INTO cnt
                FROM ' || t.owner || '.' || t.table_name || '
                WHERE TO_CHAR(effective_date, ''HH24'') = ''02''
                  AND TRUNC(effective_date) IN (
                      SELECT CASE WHEN yr < 2007 THEN NEXT_DAY(TO_DATE(yr||''-04-01'',''YYYY-MM-DD'')-1,''SUNDAY'')
                                  ELSE NEXT_DAY(TO_DATE(yr||''-03-01'',''YYYY-MM-DD'')-1,''SUNDAY'')+7 END
                      FROM (SELECT EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE,''YYYY''),-12*(LEVEL-1))) yr
                            FROM dual CONNECT BY LEVEL <= 50)
                  );
                IF cnt > 0 THEN
                    INSERT INTO temp_invalid_dst_tables VALUES (''' || t.owner || ''', ''' || t.table_name || ''', cnt);
                END IF;
            END;';
    END LOOP;
    COMMIT;
END;
/

--Step 2. List the tables
SELECT table_owner, table_name, row_count
FROM temp_invalid_dst_tables
ORDER BY table_owner, table_name;


--Step X. Update all the tables

DECLARE
    CURSOR c_tables IS
        SELECT owner, table_name
        FROM all_tables t
        WHERE table_name LIKE 'CONSUMER%'
          AND EXISTS (SELECT 1 FROM all_tab_columns c
                      WHERE c.owner = t.owner
                        AND c.table_name = t.table_name
                        AND c.column_name = 'EFFECTIVE_DATE')
          AND EXISTS (SELECT 1 FROM all_tab_columns c
                      WHERE c.owner = t.owner
                        AND c.table_name = t.table_name
                        AND c.column_name = 'RECORD_EXPIRE_DATE');

    TYPE dst_dates_t IS TABLE OF DATE;
    v_dst_dates dst_dates_t := dst_dates_t();

    v_sql             VARCHAR2(32767);
    v_col_list        VARCHAR2(32767);
    v_sel_list        VARCHAR2(32767);
    v_date_literal    VARCHAR2(200);
    v_cnt             NUMBER;
BEGIN
    -- Generate DST start dates (US rules, last 50 years)
    FOR i IN 1..50 LOOP
        DECLARE
            v_year NUMBER := EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), -12 * (i - 1)));
            v_dst  DATE;
        BEGIN
            IF v_year < 2007 THEN
                v_dst := NEXT_DAY(TO_DATE(v_year || '-04-01', 'YYYY-MM-DD') - 1, 'SUNDAY');
            ELSE
                v_dst := NEXT_DAY(TO_DATE(v_year || '-03-01', 'YYYY-MM-DD') - 1, 'SUNDAY') + 7;
            END IF;
            v_dst_dates.EXTEND;
            v_dst_dates(v_dst_dates.LAST) := v_dst;
        END;
    END LOOP;

    -- Process each table
    FOR rec IN c_tables LOOP
        -- Build full column list (all columns)
        v_col_list := NULL;
        FOR col IN (SELECT column_name
                    FROM all_tab_columns
                    WHERE owner = rec.owner
                      AND table_name = rec.table_name
                    ORDER BY column_id)
        LOOP
            IF v_col_list IS NOT NULL THEN
                v_col_list := v_col_list || ', ';
            END IF;
            v_col_list := v_col_list || col.column_name;
        END LOOP;

        -- Build SELECT list: effective_date +1 hour, all others unchanged
        v_sel_list := NULL;
        FOR col IN (SELECT column_name
                    FROM all_tab_columns
                    WHERE owner = rec.owner
                      AND table_name = rec.table_name
                    ORDER BY column_id)
        LOOP
            IF v_sel_list IS NOT NULL THEN
                v_sel_list := v_sel_list || ', ';
            END IF;
            IF col.column_name = 'EFFECTIVE_DATE' THEN
                v_sel_list := v_sel_list || 'effective_date + INTERVAL ''1'' HOUR';
            ELSE
                v_sel_list := v_sel_list || col.column_name;
            END IF;
        END LOOP;

        -- Process each DST date
        FOR i IN 1..v_dst_dates.COUNT LOOP
            v_date_literal := 'TO_DATE(''' || TO_CHAR(v_dst_dates(i), 'YYYY-MM-DD HH24:MI:SS') ||
                              ''', ''YYYY-MM-DD HH24:MI:SS'')';

            -- Count invalid rows (only active rows)
            v_sql := '
                SELECT COUNT(*)
                FROM ' || rec.owner || '.' || rec.table_name || '
                WHERE effective_date >= ' || v_date_literal || ' + INTERVAL ''2'' HOUR
                  AND effective_date <  ' || v_date_literal || ' + INTERVAL ''3'' HOUR
                  AND (record_expire_date > SYSDATE OR record_expire_date IS NULL)';
            EXECUTE IMMEDIATE v_sql INTO v_cnt;
            IF v_cnt = 0 THEN
                CONTINUE;
            END IF;

            -- Insert adjusted rows (copies original RECORD_EXPIRE_DATE)
            v_sql := '
                INSERT INTO ' || rec.owner || '.' || rec.table_name || ' (' || v_col_list || ')
                SELECT ' || v_sel_list || '
                FROM ' || rec.owner || '.' || rec.table_name || '
                WHERE effective_date >= ' || v_date_literal || ' + INTERVAL ''2'' HOUR
                  AND effective_date <  ' || v_date_literal || ' + INTERVAL ''3'' HOUR
                  AND (record_expire_date > SYSDATE OR record_expire_date IS NULL)';
            EXECUTE IMMEDIATE v_sql;

            -- Expire original rows
            v_sql := '
                UPDATE ' || rec.owner || '.' || rec.table_name || '
                SET record_expire_date = SYSTIMESTAMP
                WHERE effective_date >= ' || v_date_literal || ' + INTERVAL ''2'' HOUR
                  AND effective_date <  ' || v_date_literal || ' + INTERVAL ''3'' HOUR
                  AND (record_expire_date > SYSDATE OR record_expire_date IS NULL)';
            EXECUTE IMMEDIATE v_sql;
        END LOOP;
    END LOOP;
    -- No commit    must run COMMIT or ROLLBACK manually
END;
/
--ROLLBACK;
--COMMIT;

--Additional queries

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
WHERE TO_CHAR(a.effective_date, 'HH24') = '02'  -- hour is between 2 and 2.59
     AND (a.RECORD_EXPIRE_DATE > SYSDATE)       --active records
ORDER BY  
a.effective_date DESC;
