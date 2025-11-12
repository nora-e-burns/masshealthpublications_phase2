-- ============================================================================
-- TABLE FORMATTING AND CLEANUP FOR DOCS_CHUNKS_TABLE
-- ============================================================================



-- Clean relative paths (remove leading dots)
UPDATE DOCS_CHUNKS_TABLE
SET RELATIVE_PATH = REGEXP_REPLACE(RELATIVE_PATH, '^\\.', '')
WHERE RELATIVE_PATH LIKE '.%';

-- ============================================================================
-- EXTRACT EFFECTIVE DATES FROM FILE PATHS
-- ============================================================================

-- Create new column for effective code extraction
ALTER TABLE docs_chunks_table ADD COLUMN eff_code_final VARCHAR(255);

-- Extract effective dates from file paths
UPDATE docs_chunks_table
SET eff_code_final =
  COALESCE(
    CASE
      WHEN (
        (POSITION('eff.', relative_path) > 0 AND POSITION('.pdf', relative_path) > POSITION('eff.', relative_path))
        OR
        (POSITION('effective.', relative_path) > 0 AND POSITION('.pdf', relative_path) > POSITION('effective.', relative_path))
      )
      THEN
        CASE
          WHEN POSITION('eff.', relative_path) > 0 THEN
            CASE
              WHEN REGEXP_LIKE(
                SUBSTR(
                  relative_path,
                  POSITION('eff.', relative_path) + 4,
                  POSITION('.pdf', relative_path) - (POSITION('eff.', relative_path) + 4)
                ),
                '^[0-9]{1,4}\.[0-9]{1,4}\.[0-9]{2,4}$'
              ) THEN
                REPLACE(
                  SUBSTR(
                    relative_path,
                    POSITION('eff.', relative_path) + 4,
                    POSITION('.pdf', relative_path) - (POSITION('eff.', relative_path) + 4)
                  ),
                  '.', '/'
                )
              ELSE
                SUBSTR(
                  relative_path,
                  POSITION('eff.', relative_path) + 4,
                  POSITION('.pdf', relative_path) - (POSITION('eff.', relative_path) + 4)
                )
            END
          ELSE
            CASE
              WHEN REGEXP_LIKE(
                SUBSTR(
                  relative_path,
                  POSITION('effective.', relative_path) + 10,
                  POSITION('.pdf', relative_path) - (POSITION('effective.', relative_path) + 10)
                ),
                '^[0-9]{1,4}\.[0-9]{1,4}\.[0-9]{2,4}$'
              ) THEN
                REPLACE(
                  SUBSTR(
                    relative_path,
                    POSITION('effective.', relative_path) + 10,
                    POSITION('.pdf', relative_path) - (POSITION('effective.', relative_path) + 10)
                  ),
                  '.', '/'
                )
              ELSE
                SUBSTR(
                  relative_path,
                  POSITION('effective.', relative_path) + 10,
                  POSITION('.pdf', relative_path) - (POSITION('effective.', relative_path) + 10)
                )
            END
        END
      ELSE NULL
    END,
    CASE
      WHEN REGEXP_LIKE(
        relative_path,
        '(eff|effective)[ ._-]+([0-9]{1,4}[.-][0-9]{1,4}[.-][0-9]{2,4})\.(pdf|docx)'
      )
      THEN REPLACE(
        REPLACE(
          REGEXP_SUBSTR(
            relative_path,
            '(eff|effective)[ ._-]+([0-9]{1,4}[.-][0-9]{1,4}[.-][0-9]{2,4})',
            1, 1, 'c', 1
          ),
          '-', '/'
        ),
        '.', '/'
      )
      ELSE NULL
    END,
    CASE
      WHEN (
        (POSITION('eff ', relative_path) > 0 AND (POSITION('.docx', relative_path) > POSITION('eff ', relative_path) OR POSITION('.pdf', relative_path) > POSITION('eff ', relative_path)))
        OR
        (POSITION('effective ', relative_path) > 0 AND (POSITION('.docx', relative_path) > POSITION('effective ', relative_path) OR POSITION('.pdf', relative_path) > POSITION('effective ', relative_path)))
      )
      THEN
        CASE
          WHEN POSITION('eff ', relative_path) > 0 AND POSITION('.docx', relative_path) > POSITION('eff ', relative_path) THEN
            CASE
              WHEN REGEXP_LIKE(
                SUBSTR(
                  relative_path,
                  POSITION('eff ', relative_path) + 4,
                  POSITION('.docx', relative_path) - (POSITION('eff ', relative_path) + 4)
                ),
                '^[0-9]{1,4}-[0-9]{1,2}-[0-9]{2,4}$'
              ) THEN
                REPLACE(
                  SUBSTR(
                    relative_path,
                    POSITION('eff ', relative_path) + 4,
                    POSITION('.docx', relative_path) - (POSITION('eff ', relative_path) + 4)
                  ),
                  '-', '/'
                )
              ELSE
                SUBSTR(
                  relative_path,
                  POSITION('eff ', relative_path) + 4,
                  POSITION('.docx', relative_path) - (POSITION('eff ', relative_path) + 4)
                )
            END
          WHEN POSITION('eff ', relative_path) > 0 AND POSITION('.pdf', relative_path) > POSITION('eff ', relative_path) THEN
            CASE
              WHEN REGEXP_LIKE(
                SUBSTR(
                  relative_path,
                  POSITION('eff ', relative_path) + 4,
                  POSITION('.pdf', relative_path) - (POSITION('eff ', relative_path) + 4)
                ),
                '^[0-9]{1,4}-[0-9]{1,2}-[0-9]{2,4}$'
              ) THEN
                REPLACE(
                  SUBSTR(
                    relative_path,
                    POSITION('eff ', relative_path) + 4,
                    POSITION('.pdf', relative_path) - (POSITION('eff ', relative_path) + 4)
                  ),
                  '-', '/'
                )
              ELSE
                SUBSTR(
                  relative_path,
                  POSITION('eff ', relative_path) + 4,
                  POSITION('.pdf', relative_path) - (POSITION('eff ', relative_path) + 4)
                )
            END
          WHEN POSITION('effective ', relative_path) > 0 AND POSITION('.docx', relative_path) > POSITION('effective ', relative_path) THEN
            CASE
              WHEN REGEXP_LIKE(
                SUBSTR(
                  relative_path,
                  POSITION('effective ', relative_path) + 10,
                  POSITION('.docx', relative_path) - (POSITION('effective ', relative_path) + 10)
                ),
                '^[0-9]{1,4}-[0-9]{1,2}-[0-9]{2,4}$'
              ) THEN
                REPLACE(
                  SUBSTR(
                    relative_path,
                    POSITION('effective ', relative_path) + 10,
                    POSITION('.docx', relative_path) - (POSITION('effective ', relative_path) + 10)
                  ),
                  '-', '/'
                )
              ELSE
                SUBSTR(
                  relative_path,
                  POSITION('effective ', relative_path) + 10,
                  POSITION('.docx', relative_path) - (POSITION('effective ', relative_path) + 10)
                )
            END
          WHEN POSITION('effective ', relative_path) > 0 AND POSITION('.pdf', relative_path) > POSITION('effective ', relative_path) THEN
            CASE
              WHEN REGEXP_LIKE(
                SUBSTR(
                  relative_path,
                  POSITION('effective ', relative_path) + 10,
                  POSITION('.pdf', relative_path) - (POSITION('effective ', relative_path) + 10)
                ),
                '^[0-9]{1,4}-[0-9]{1,2}-[0-9]{2,4}$'
              ) THEN
                REPLACE(
                  SUBSTR(
                    relative_path,
                    POSITION('effective ', relative_path) + 10,
                    POSITION('.pdf', relative_path) - (POSITION('effective ', relative_path) + 10)
                  ),
                  '-', '/'
                )
              ELSE
                SUBSTR(
                  relative_path,
                  POSITION('effective ', relative_path) + 10,
                  POSITION('.pdf', relative_path) - (POSITION('effective ', relative_path) + 10)
                )
            END
          ELSE NULL
        END
      ELSE NULL
    END
  );

-- ============================================================================
-- CLEAN UP EXTRACTED EFFECTIVE CODES
-- ============================================================================

-- Remove unwanted words
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, 'emergency', '');
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, 'Emergency', '');
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, '/Corrected', '');
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, '/clean', '');
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, '/Emergency', '');
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, 'clean', '');
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, '/paperwork', '');

-- Standardize separators
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, '.', '/');
UPDATE docs_chunks_table SET eff_code_final = REPLACE(eff_code_final, '-', '/');

-- Convert YYYY/MM/DD to MM/DD/YYYY format
UPDATE docs_chunks_table
SET eff_code_final = REGEXP_REPLACE(eff_code_final, '^([0-9]{4})/([0-9]{2})/([0-9]{2})$', '\\2/\\3/\\1')
WHERE eff_code_final REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$';

UPDATE docs_chunks_table
SET eff_code_final = REGEXP_REPLACE(eff_code_final, '^([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})$', '\\2/\\3/\\1')
WHERE eff_code_final REGEXP '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$';

-- Clean up extra characters and formatting
UPDATE docs_chunks_table SET EFF_CODE_FINAL = RTRIM(RTRIM(EFF_CODE_FINAL, '/'), '_');
UPDATE docs_chunks_table SET EFF_CODE_FINAL = REGEXP_REPLACE(EFF_CODE_FINAL, ' \\(\\d+\\)$', '');
UPDATE docs_chunks_table SET EFF_CODE_FINAL = REGEXP_REPLACE(EFF_CODE_FINAL, ' \\d+ of \\d+$', '');
UPDATE docs_chunks_table SET EFF_CODE_FINAL = REGEXP_REPLACE(EFF_CODE_FINAL, '\\s+\\d+$', '');
UPDATE docs_chunks_table SET EFF_CODE_FINAL = REGEXP_REPLACE(EFF_CODE_FINAL, '\\(\\d\\)', '');

-- Handle second dates (AND logic)
ALTER TABLE docs_chunks_table ADD COLUMN eff_code_second VARCHAR(255);

UPDATE docs_chunks_table
SET
  EFF_CODE_SECOND = CASE 
    WHEN POSITION(' and ' IN EFF_CODE_FINAL) > 0
      THEN SUBSTR(EFF_CODE_FINAL, POSITION(' and ' IN EFF_CODE_FINAL) + 5)
      ELSE NULL
  END,
  EFF_CODE_FINAL = CASE
    WHEN POSITION(' and ' IN EFF_CODE_FINAL) > 0
      THEN LEFT(EFF_CODE_FINAL, POSITION(' and ' IN EFF_CODE_FINAL) - 1)
      ELSE EFF_CODE_FINAL
  END;

-- Clean both date columns
UPDATE docs_chunks_table
SET 
  EFF_CODE_FINAL = REGEXP_REPLACE(EFF_CODE_FINAL, '[A-Za-z]', ''),
  EFF_CODE_SECOND = REGEXP_REPLACE(EFF_CODE_SECOND, '[A-Za-z]', '');

UPDATE docs_chunks_table
SET 
  EFF_CODE_FINAL = LTRIM(RTRIM(EFF_CODE_FINAL)),
  EFF_CODE_SECOND = LTRIM(RTRIM(EFF_CODE_SECOND));

-- Fix double slashes and add missing days
UPDATE docs_chunks_table SET EFF_CODE_FINAL = REPLACE(EFF_CODE_FINAL, '//', '/');
UPDATE docs_chunks_table SET eff_code_final = eff_code_final || '/01' WHERE eff_code_final REGEXP '^[0-9]{4}/[0-9]{2}$';
UPDATE docs_chunks_table SET eff_code_final = REGEXP_REPLACE(eff_code_final, '^([0-9]{1,2})/([0-9]{4})$', '\\1/1/\\2') WHERE eff_code_final REGEXP '^[0-9]{1,2}/[0-9]{4}$';

-- ============================================================================
-- CONVERT TO DATE FORMAT
-- ============================================================================

-- Create date column
ALTER TABLE docs_chunks_table ADD COLUMN eff_code_final_date DATE;

-- Convert string dates to proper DATE format
UPDATE docs_chunks_table
SET eff_code_final_date = COALESCE(
    TRY_TO_DATE(TRIM(eff_code_final), 'M/D/YY'),
    TRY_TO_DATE(TRIM(eff_code_final), 'MM/D/YY'),
    TRY_TO_DATE(TRIM(eff_code_final), 'M/DD/YY'),
    TRY_TO_DATE(TRIM(eff_code_final), 'MM/DD/YY'),
    TRY_TO_DATE(TRIM(eff_code_final), 'M/D/YYYY'),
    TRY_TO_DATE(TRIM(eff_code_final), 'MM/D/YYYY'),
    TRY_TO_DATE(TRIM(eff_code_final), 'M/DD/YYYY'),
    TRY_TO_DATE(TRIM(eff_code_final), 'MM/DD/YYYY')
);

-- ============================================================================
-- CLEAN CHUNK TEXT
-- ============================================================================

-- Clean chunk content
UPDATE docs_chunks_table SET chunk = REPLACE(chunk, '\\n', '\n') WHERE chunk LIKE '%\\n%';
UPDATE docs_chunks_table SET CHUNK = REGEXP_REPLACE(CHUNK, '\\|\\|\\|', '');
UPDATE docs_chunks_table SET CHUNK = REGEXP_REPLACE(CHUNK, '{"content":"', '');
UPDATE docs_chunks_table SET CHUNK = REGEXP_REPLACE(CHUNK, '","metadata":', '');

-- ============================================================================
-- CREATE FILE PATH AFTER FILES COLUMN
-- ============================================================================

-- Add new column for file path after "files/"
ALTER TABLE docs_chunks_table ADD COLUMN file_path_after_files VARCHAR(16777216);

-- Extract path after "files/"
UPDATE docs_chunks_table
SET file_path_after_files = CASE 
    WHEN POSITION('files/' IN file_url) > 0 
    THEN SUBSTR(file_url, POSITION('files/' IN file_url) + 6)
    ELSE NULL
END;

-- ============================================================================
-- CLEANUP - DROP INTERMEDIATE COLUMNS
-- ============================================================================

-- Drop intermediate columns that are no longer needed
ALTER TABLE docs_chunks_table DROP COLUMN IF EXISTS eff_code_final;
ALTER TABLE docs_chunks_table DROP COLUMN IF EXISTS eff_code_second;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Check the results
SELECT 
    COUNT(*) as total_rows,
    COUNT(eff_code_final_date) as rows_with_dates,
    COUNT(file_path_after_files) as rows_with_file_paths
FROM docs_chunks_table;

-- Sample the cleaned data
SELECT 
    LEFT(relative_path, 50) as relative_path_preview,
    eff_code_final_date,
    LEFT(file_path_after_files, 50) as file_path_preview,
    LEFT(chunk, 100) as chunk_preview
FROM docs_chunks_table
WHERE eff_code_final_date IS NOT NULL
LIMIT 10;


-- CREATE CORTEX SEARCH SERVICE
create or replace CORTEX SEARCH SERVICE MH_PUBLICATIONS_SEARCH_SERVICE
ON chunk
ATTRIBUTES RELATIVE_PATH, CHUNK_ORDER, EFF_CODE_FINAL_DATE
warehouse = AIPILOT_WH
TARGET_LAG = '365 DAYS'
as (
    select chunk,
        relative_path,
        chunk_order,
        file_url,
        eff_code_final_date
    from docs_chunks_table
);

=================== 

-- Step 1: Create the effdatefinal2 column
ALTER TABLE MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE 
ADD COLUMN effdatefinal2 DATE;

-- Step 2: Populate effdatefinal2 with extracted dates
UPDATE MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE
SET effdatefinal2 = 
    CASE 
        WHEN eff_code_final_date IS NULL 
             AND REGEXP_SUBSTR(relative_path, '[0-9]{4}\\.[0-9]{1,2}\\.[0-9]{1,2}') IS NOT NULL THEN
            TO_DATE(
                REPLACE(REGEXP_SUBSTR(relative_path, '[0-9]{4}\\.[0-9]{1,2}\\.[0-9]{1,2}'), '.', '-'),
                'YYYY-MM-DD'
            )
        ELSE NULL
    END
WHERE eff_code_final_date IS NULL;

-- Step 3: Copy extracted dates to eff_code_final_date where it's NULL
UPDATE MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE
SET eff_code_final_date = effdatefinal2
WHERE eff_code_final_date IS NULL 
    AND effdatefinal2 IS NOT NULL;

-- Step 4: Drop the temporary column
ALTER TABLE MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE 
DROP COLUMN effdatefinal2;

-- Step 5: Verify the results
SELECT 
    relative_path,
    eff_code_final_date
FROM MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE
WHERE REGEXP_COUNT(relative_path, '[0-9]{4}\\.[0-9]{1,2}\\.[0-9]{1,2}') > 0
    AND eff_code_final_date IS NOT NULL
ORDER BY eff_code_final_date DESC
LIMIT 20;

============ 

UPDATE MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE
SET eff_code_final_date = 
    COALESCE(
        -- Pattern 1: YYYY-MM-DD (2024-10-01)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '20[0-9]{2}-[0-9]{1,2}-[0-9]{1,2}'), 'YYYY-MM-DD'),
        
        -- Pattern 2: MM.DD.YYYY (02.01.2018, 01.18.2013)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '[0-9]{1,2}\\.[0-9]{1,2}\\.(20[0-9]{2})'), 'MM.DD.YYYY'),
        
        -- Pattern 3: MM.DD.YY (03.31.15, 09.29.15, 12.31.15, etc.) - fixed
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '\\b[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{2}\\b'), 'MM.DD.YY'),
        
        -- Pattern 4: M.DD.YY (1.25.18)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '\\b[0-9]{1}\\.[0-9]{2}\\.[0-9]{2}\\b'), 'M.DD.YY'),
        
        -- Pattern 5: MMDDYY 6-digit dates (022814, 092713, 122214, etc.)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '\\b[0-1][0-9][0-3][0-9][0-9]{2}\\b'), 'MMDDYY'),
        
        -- Pattern 6: Alternative MMDDYY pattern (102314, 020113)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '\\b[0-9]{2}[0-3][0-9][0-9]{2}\\b'), 'MMDDYY'),
        
        -- Pattern 7: YYMMDD format (170701)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '\\b[0-9]{2}[0-1][0-9][0-3][0-9]\\b'), 'YYMMDD'),
        
        -- Pattern 8: MM.DD.YY with word boundaries (09.04.15)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '\\b[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\b'), 'MM.DD.YY'),
        
        -- Pattern 9: YYYY format (standalone 4-digit year)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '\\b(20[0-9]{2})\\b') || '-01-01', 'YYYY-MM-DD')
    )
WHERE eff_code_final_date IS NULL;

UPDATE MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE
SET eff_code_final_date = 
    COALESCE(
        -- Pattern 1: YYYY-MM-DD (2024-10-01)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '20[0-9]{2}-[0-9]{1,2}-[0-9]{1,2}'), 'YYYY-MM-DD'),
        
        -- Pattern 2: MM.DD.YYYY (02.01.2018, 01.18.2013)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '[0-9]{1,2}\\.[0-9]{1,2}\\.(20[0-9]{2})'), 'MM.DD.YYYY'),
        
        -- Pattern 3: MM.DD.YY - More specific for your cases
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}'), 'MM.DD.YY'),
        
        -- Pattern 4: M.DD.YY (1.25.18) 
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '[0-9]{1}\\.[0-9]{2}\\.[0-9]{2}'), 'M.DD.YY'),
        
        -- Pattern 5: MMDDYY 6-digit - More restrictive to avoid conflicts
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '[0-1][0-9][0-3][0-9][0-9]{2}'), 'MMDDYY'),
        
        -- Pattern 6: YYMMDD format (170701)
        TRY_TO_DATE(REGEXP_SUBSTR(relative_path, '[0-9]{2}[0-1][0-9][0-3][0-9]'), 'YYMMDD')
    )
WHERE eff_code_final_date IS NULL;

======= 
-- Update eff_code_final_date to NULL for impossible dates
UPDATE MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE
SET eff_code_final_date = NULL
WHERE eff_code_final_date IS NOT NULL
    AND (
        eff_code_final_date < '1900-01-01' OR
        eff_code_final_date > '2025-12-31'
    );

-- Verification query to see what was changed
SELECT 
    'Before cleanup' as status,
    COUNT(*) as total_records,
    COUNT(eff_code_final_date) as records_with_dates,
    COUNT(*) - COUNT(eff_code_final_date) as null_dates
FROM MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE

UNION ALL

-- Show the range of dates that remain
SELECT 
    'After cleanup' as status,
    COUNT(*) as total_records,
    COUNT(eff_code_final_date) as records_with_dates,
    COUNT(*) - COUNT(eff_code_final_date) as null_dates
FROM MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE;

-- Show the date range that remains
SELECT 
    MIN(eff_code_final_date) as earliest_date,
    MAX(eff_code_final_date) as latest_date,
    COUNT(eff_code_final_date) as valid_dates_remaining
FROM MH_PUBLICATIONS.DATA.DOCS_CHUNKS_TABLE
WHERE eff_code_final_date IS NOT NULL;
    