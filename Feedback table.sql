-- Create feedback table in Snowflake
CREATE OR REPLACE TABLE MH_PUBLICATIONS.DATA.CHAT_FEEDBACK (
    feedback_id VARCHAR DEFAULT UUID_STRING(),
    session_id VARCHAR,
    message_index INTEGER,
    user_question VARCHAR,
    assistant_response VARCHAR,
    feedback_type VARCHAR, -- 'positive' or 'negative'
    feedback_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    user_id VARCHAR DEFAULT 'anonymous'
);