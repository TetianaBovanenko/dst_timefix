--Query that lists all tables with invalid DST (within the current environment):

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

SELECT table_owner, table_name, row_count
FROM temp_invalid_dst_tables
ORDER BY table_owner, table_name;
