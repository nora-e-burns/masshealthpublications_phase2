-- MASS_SEARCH.DATA.FILE_TREE



CREATE OR REPLACE TABLE mass_documents (
    staged_file_path VARCHAR,       -- Full path of the file in the Snowflake stage (acts as document identifier)
    file_name VARCHAR,              -- Derived file name
    chunk_id VARCHAR,               -- Unique ID for each text chunk
    chunk_sequence_number INT,      -- Order of the chunk within its parent document
    chunk_text VARCHAR              -- The actual text content of the chunk
);

-- Enable Change Tracking on the table, required for Cortex Search Service
ALTER TABLE mass_documents SET CHANGE_TRACKING = TRUE;



-- -- Enable Directory Table on your stage
ALTER STAGE MH_PUBLICATIONS.DATA.UPLOAD_070225 SET DIRECTORY = (ENABLE = TRUE);

-- -- Refresh the stage's directory table to pick up the latest file list
-- -- Run this especially if files were recently added or the directory table was just enabled.
-- ALTER STAGE MASS_SEARCH.DATA.FILE_TREE REFRESH;




create or replace TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK_ORDER INTEGER, -- Order of the chunk in the original document
    CHUNK VARCHAR(16777216) -- Piece of text
);

-- USE CORTEX PARSE_DOCUMENT TO READ AND USE FUNCTION CREATED TO CHUNK
insert into docs_chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk_order, chunk)

    select relative_path, 
            size,
            file_url, 
            build_scoped_file_url(@upload_070225, relative_path) as scoped_file_url,
            func.chunk_order as chunk_order,
            func.chunk as chunk
    from 
        directory(@upload_070225),
        TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@upload_070225, relative_path, {'mode': 'LAYOUT'})))) as func;

-- CHECK CHUNKS TABLE
select *
from docs_chunks_table;
     

