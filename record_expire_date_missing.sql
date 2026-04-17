--SQL query to identify in which tables RECORD_EXPIRE_DATE column is missing:


-- create the temporary table with an extra column
DROP TABLE temp_table_counts PURGE;
CREATE GLOBAL TEMPORARY TABLE temp_table_counts (
    table_owner            VARCHAR2(128),
    table_name             VARCHAR2(128),
    row_count              NUMBER,
    has_record_expire_date VARCHAR2(1)   -- 'Y' or 'N'
) ON COMMIT PRESERVE ROWS;

--tables that have EFFECTIVE_DATE and count invalid DST rows
BEGIN
    DELETE FROM temp_table_counts;
    FOR t IN (SELECT owner, table_name 
              FROM all_tables 
              WHERE table_name LIKE 'CONSUMER%'
              AND EXISTS (SELECT 1 FROM all_tab_columns 
                          WHERE owner = all_tables.owner 
                          AND table_name = all_tables.table_name 
                          AND column_name = 'EFFECTIVE_DATE'))
    LOOP
        DECLARE
            v_has_expire CHAR(1) := 'N';
        BEGIN
            -- Check if RECORD_EXPIRE_DATE exists
            BEGIN
                SELECT 'Y' INTO v_has_expire
                FROM all_tab_columns
                WHERE owner = t.owner
                  AND table_name = t.table_name
                  AND column_name = 'RECORD_EXPIRE_DATE'
                  AND ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_has_expire := 'N';
            END;
            
            -- Now count invalid rows using dynamic SQL, passing v_has_expire as literal
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
                        INSERT INTO temp_table_counts VALUES (''' || t.owner || ''', ''' || t.table_name || ''', cnt, ''' || v_has_expire || ''');
                    END IF;
                END;';
        END;
    END LOOP;
    COMMIT;
END;
/


--tables that are missing RECORD_EXPIRE_DATE
SELECT table_owner, table_name, row_count
FROM temp_table_counts
WHERE has_record_expire_date = 'N'
ORDER BY table_owner, table_name;
