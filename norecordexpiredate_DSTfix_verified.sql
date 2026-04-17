-- Step 1. Identify in which tables RECORD_EXPIRE_DATE column is missing

DROP TABLE temp_table_counts PURGE;
CREATE GLOBAL TEMPORARY TABLE temp_table_counts (
    table_owner            VARCHAR2(128),
    table_name             VARCHAR2(128),
    row_count              NUMBER,
    has_record_expire_date VARCHAR2(1)   -- 'Y' or 'N'
) ON COMMIT PRESERVE ROWS;

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
            
            -- Count invalid rows
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

-- Step 2. Query the tables that are missing RECORD_EXPIRE_DATE column
SELECT table_owner, table_name, row_count
FROM temp_table_counts
WHERE has_record_expire_date = 'N'
ORDER BY table_owner, table_name;

-- Step 3. List the tables (manually) to know which tables to be updated one by one in Step 4:


-- Step 4. Fix the data for a specific table. Creates a backup table named ASP4545_DATABACKUP_<table_name>.Inserts affected rows into backup before update, with a timestamp of the script run.

DECLARE
    v_owner          VARCHAR2(128) := 'XXX';      -- Change to actual owner
    v_table_name     VARCHAR2(128) := 'XXX'; -- Change to the current one
    v_backup_table   VARCHAR2(128) := 'DATABACKUP_' || v_table_name;
    v_run_timestamp  TIMESTAMP := SYSTIMESTAMP;

    TYPE dst_dates_t IS TABLE OF DATE;
    v_dst_dates dst_dates_t := dst_dates_t();

    v_sql             VARCHAR2(32767);
    v_col_list        VARCHAR2(32767);
    v_date_literal    VARCHAR2(200);
    v_cnt             NUMBER;
    v_table_exists    NUMBER;
BEGIN
    -- Generate DST start dates
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

    -- Build comma-separated column list
    FOR col IN (SELECT column_name
                FROM all_tab_columns
                WHERE owner = v_owner
                  AND table_name = v_table_name
                ORDER BY column_id)
    LOOP
        IF v_col_list IS NOT NULL THEN
            v_col_list := v_col_list || ', ';
        END IF;
        v_col_list := v_col_list || col.column_name;
    END LOOP;

    -- Create backup table if it does not exist
    BEGIN
        -- Create as a copy of the source table structure
        v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || v_owner || '.' || v_table_name || ' WHERE 1=0';
        EXECUTE IMMEDIATE v_sql;
        -- Add the backup_date column
        v_sql := 'ALTER TABLE ' || v_backup_table || ' ADD (backup_date TIMESTAMP)';
        EXECUTE IMMEDIATE v_sql;
        DBMS_OUTPUT.PUT_LINE('Backup table created: ' || v_backup_table);
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -955 THEN  -- if table exists continue
                DBMS_OUTPUT.PUT_LINE('Backup table already exists: ' || v_backup_table || '   will append rows.');
            ELSE
                RAISE;
            END IF;
    END;

    -- Process each DST date
    FOR i IN 1..v_dst_dates.COUNT LOOP
        v_date_literal := 'TO_DATE(''' || TO_CHAR(v_dst_dates(i), 'YYYY-MM-DD HH24:MI:SS') ||
                          ''', ''YYYY-MM-DD HH24:MI:SS'')';

        -- Count affected rows
        v_sql := '
            SELECT COUNT(*)
            FROM ' || v_owner || '.' || v_table_name || '
            WHERE effective_date >= ' || v_date_literal || ' + INTERVAL ''2'' HOUR
              AND effective_date <  ' || v_date_literal || ' + INTERVAL ''3'' HOUR';
        EXECUTE IMMEDIATE v_sql INTO v_cnt;
        IF v_cnt = 0 THEN
            CONTINUE;
        END IF;

        -- Insert affected rows into backup table
        v_sql := '
            INSERT INTO ' || v_backup_table || ' (' || v_col_list || ', backup_date)
            SELECT ' || v_col_list || ', :1
            FROM ' || v_owner || '.' || v_table_name || '
            WHERE effective_date >= ' || v_date_literal || ' + INTERVAL ''2'' HOUR
              AND effective_date <  ' || v_date_literal || ' + INTERVAL ''3'' HOUR';
        EXECUTE IMMEDIATE v_sql USING v_run_timestamp;

        -- Update original rows: add 1 hour to effective_date
        v_sql := '
            UPDATE ' || v_owner || '.' || v_table_name || '
            SET effective_date = effective_date + INTERVAL ''1'' HOUR
            WHERE effective_date >= ' || v_date_literal || ' + INTERVAL ''2'' HOUR
              AND effective_date <  ' || v_date_literal || ' + INTERVAL ''3'' HOUR';
        EXECUTE IMMEDIATE v_sql;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Script run timestamp: ' || TO_CHAR(v_run_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF'));
    DBMS_OUTPUT.PUT_LINE('Rows inserted into backup table: ' || v_backup_table);
    DBMS_OUTPUT.PUT_LINE('Rows updated in source table: ' || v_table_name);
    DBMS_OUTPUT.PUT_LINE('Please VERIFY the changes. Then run COMMIT; or ROLLBACK;');
END;
/


-- After running the block above:
--   1. Query the backup table to see what was changed:
--        SELECT * FROM DATABACKUP_XXX;
--   2. Query the source table to confirm corrections.
--   3. If correct, run:  COMMIT;
--   4. If incorrect, run: ROLLBACK;
