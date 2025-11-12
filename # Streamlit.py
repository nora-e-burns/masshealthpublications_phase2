# Streamlit 
# RAG 
import json
import logging
import re
import streamlit as st
import uuid
import base64
from datetime import datetime, date
from snowflake.snowpark.functions import call_udf, concat, lit
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the active Snowflake session
session = get_active_session()
logging.info("Active Snowflake session retrieved.")

# Define database, schema, and search service names
# Update these parameters as needed for your specific Snowflake setup
db_name = 'MH_PUBLICATIONS'
schema_name = 'DATA'
search_service_name = 'MH_PUBLICATIONS_SEARCH_SERVICE'

# Set up the Cortex Search Service Root
root = Root(session)

#------------------------------------------------------------------------------
# DYNAMIC CHUNK CALCULATION FUNCTIONS
#------------------------------------------------------------------------------

def calculate_question_complexity(question):
    """
    Calculate the complexity of a question based on various factors.
    Returns a complexity score between 1 and 10.
    """
    complexity_score = 1  # Base score
    
    # Factor 1: Question length (longer questions tend to be more complex)
    word_count = len(question.split())
    if word_count > 50:
        complexity_score += 3
    elif word_count > 30:
        complexity_score += 2
    elif word_count > 15:
        complexity_score += 1
    
    # Factor 2: Multiple question indicators
    question_indicators = ['?', 'what', 'how', 'why', 'when', 'where', 'who', 'which']
    question_count = sum(1 for indicator in question_indicators if indicator in question.lower())
    if question_count > 3:
        complexity_score += 2
    elif question_count > 2:
        complexity_score += 1
    
    # Factor 3: Complex keywords that suggest detailed answers needed
    complex_keywords = [
        'compare', 'contrast', 'analyze', 'explain', 'describe', 'detail', 'comprehensive',
        'thorough', 'complete', 'all', 'every', 'various', 'different', 'multiple',
        'process', 'procedure', 'steps', 'requirements', 'criteria', 'conditions',
        'eligibility', 'qualification', 'documentation', 'application', 'enrollment',
        'benefits', 'coverage', 'services', 'options', 'alternatives', 'exceptions'
    ]
    
    complex_keyword_count = sum(1 for keyword in complex_keywords if keyword in question.lower())
    if complex_keyword_count > 5:
        complexity_score += 3
    elif complex_keyword_count > 3:
        complexity_score += 2
    elif complex_keyword_count > 1:
        complexity_score += 1
    
    # Factor 4: Conjunctions suggesting multiple parts
    conjunctions = [' and ', ' or ', ' but ', ' also ', ' additionally', ' furthermore', ' moreover']
    conjunction_count = sum(1 for conj in conjunctions if conj in question.lower())
    if conjunction_count > 2:
        complexity_score += 2
    elif conjunction_count > 0:
        complexity_score += 1
    
    # Factor 5: Specific complex question patterns
    complex_patterns = [
        r'what are (?:all )?the .* for',  # "what are all the requirements for"
        r'how (?:do|can) i .* and .*',    # "how do i apply and what documents"
        r'what is the difference between',  # comparison questions
        r'can you (?:explain|describe|list) (?:all|the)',  # comprehensive requests
        r'what (?:steps|process|procedure)',  # process questions
        r'(?:list|show|tell me about) (?:all|every|the various)'  # comprehensive lists
    ]
    
    pattern_matches = sum(1 for pattern in complex_patterns if re.search(pattern, question.lower()))
    if pattern_matches > 0:
        complexity_score += 2
    
    # Cap the score at 10
    return min(complexity_score, 10)

def determine_chunk_count(question, base_chunks=3, max_chunks=12):
    """
    Determine the optimal number of chunks based on question complexity.
    
    Args:
        question (str): The user's question
        base_chunks (int): Minimum number of chunks
        max_chunks (int): Maximum number of chunks
    
    Returns:
        int: Number of chunks to use
    """
    complexity = calculate_question_complexity(question)
    
    # Map complexity (1-10) to chunk count (base_chunks to max_chunks)
    chunk_count = base_chunks + int((complexity - 1) * (max_chunks - base_chunks) / 9)
    
    return max(base_chunks, min(chunk_count, max_chunks))

def get_complexity_explanation(question):
    """
    Get a human-readable explanation of why a certain complexity was assigned.
    """
    complexity = calculate_question_complexity(question)
    word_count = len(question.split())
    
    explanations = []
    
    if word_count > 30:
        explanations.append(f"Long question ({word_count} words)")
    elif word_count > 15:
        explanations.append(f"Medium-length question ({word_count} words)")
    
    complex_keywords = [
        'compare', 'contrast', 'analyze', 'explain', 'describe', 'detail', 'comprehensive',
        'thorough', 'complete', 'all', 'every', 'various', 'different', 'multiple',
        'process', 'procedure', 'steps', 'requirements', 'criteria', 'conditions'
    ]
    
    found_keywords = [kw for kw in complex_keywords if kw in question.lower()]
    if found_keywords:
        explanations.append(f"Complex keywords detected: {', '.join(found_keywords[:3])}")
    
    if 'what are' in question.lower() and ('all' in question.lower() or 'every' in question.lower()):
        explanations.append("Comprehensive information request")
    
    if len(explanations) == 0:
        explanations.append("Simple, direct question")
    
    return f"Complexity: {complexity}/10 ({'; '.join(explanations)})"

#------------------------------------------------------------------------------
# CHAT HISTORY FUNCTIONS
#------------------------------------------------------------------------------

def save_chat_to_history(session_id, user_question, assistant_response, sources_used=None):
    """Save Q&A pair to CHAT_HISTORY table"""
    try:
        # Use parameterized query to handle special characters
        history_query = f"""
        INSERT INTO {db_name}.{schema_name}.CHAT_HISTORY 
        (session_id, user_question, assistant_response, sources_used, created_timestamp)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())
        """
        session.sql(history_query, params=[session_id, user_question, assistant_response, sources_used]).collect()
        return True
    except Exception as e:
        # Create table if it doesn't exist
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.CHAT_HISTORY (
                chat_id VARCHAR DEFAULT UUID_STRING(),
                session_id VARCHAR,
                user_question VARCHAR(16777216),
                assistant_response VARCHAR(16777216),
                sources_used VARCHAR(16777216),
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                user_id VARCHAR DEFAULT 'anonymous',
                PRIMARY KEY (chat_id)
            )
            """
            session.sql(create_table_query).collect()
            # Try inserting again
            session.sql(history_query, params=[session_id, user_question, assistant_response, sources_used]).collect()
            return True
        except Exception as e2:
            st.error(f"Error saving chat history: {str(e2)}")
            return False

def get_recent_chat_sessions(limit=10):
    """Get the most recent 10 chat sessions"""
    try:
        # Get distinct sessions with their latest timestamp and first question as preview
        query = f"""
        WITH ranked_chats AS (
            SELECT 
                session_id,
                user_question,
                created_timestamp,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_timestamp ASC) as rn_first,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_timestamp DESC) as rn_latest
            FROM {db_name}.{schema_name}.CHAT_HISTORY
        ),
        session_previews AS (
            SELECT 
                session_id,
                user_question as first_question,
                created_timestamp as session_start
            FROM ranked_chats 
            WHERE rn_first = 1
        ),
        latest_timestamps AS (
            SELECT 
                session_id,
                created_timestamp as last_activity
            FROM ranked_chats 
            WHERE rn_latest = 1
        )
        SELECT 
            sp.session_id,
            sp.first_question,
            sp.session_start,
            lt.last_activity
        FROM session_previews sp
        JOIN latest_timestamps lt ON sp.session_id = lt.session_id
        ORDER BY lt.last_activity DESC
        LIMIT {limit}
        """
        
        result = session.sql(query).collect()
        return [dict(row.asDict()) for row in result]
    except Exception as e:
        logging.error(f"Error fetching recent chat sessions: {str(e)}")
        return []

def load_chat_session(session_id):
    """Load a complete chat session from history"""
    try:
        query = f"""
        SELECT 
            user_question,
            assistant_response,
            sources_used,
            created_timestamp
        FROM {db_name}.{schema_name}.CHAT_HISTORY 
        WHERE session_id = ?
        ORDER BY created_timestamp ASC
        """
        
        result = session.sql(query, params=[session_id]).collect()
        return [dict(row.asDict()) for row in result]
    except Exception as e:
        logging.error(f"Error loading chat session: {str(e)}")
        return []

def delete_chat_history():
    """Delete all chat history from the database"""
    try:
        delete_query = f"""
        DELETE FROM {db_name}.{schema_name}.CHAT_HISTORY
        """
        session.sql(delete_query).collect()
        return True
    except Exception as e:
        st.error(f"Error deleting chat history: {str(e)}")
        return False

def delete_specific_chat_session(session_id):
    """Delete a specific chat session from the database"""
    try:
        delete_query = f"""
        DELETE FROM {db_name}.{schema_name}.CHAT_HISTORY 
        WHERE session_id = ?
        """
        session.sql(delete_query, params=[session_id]).collect()
        return True
    except Exception as e:
        st.error(f"Error deleting chat session: {str(e)}")
        return False

#------------------------------------------------------------------------------
# UI SETUP - SIDEBAR CONFIGURATION
#------------------------------------------------------------------------------

st.sidebar.title("Settings")

# ============================================================================
# RECENT CHATS SECTION (MOVED TO TOP)
# ============================================================================
st.sidebar.subheader("💬 Recent Chats")

# Initialize chat loading state
if "loading_chat" not in st.session_state:
    st.session_state.loading_chat = False

# Get recent chat sessions
recent_sessions = get_recent_chat_sessions(10)

if recent_sessions:
    for i, chat_session in enumerate(recent_sessions):
        session_id = chat_session['SESSION_ID']
        first_question = chat_session['FIRST_QUESTION']
        session_start = chat_session['SESSION_START']
        
        # Truncate question for display
        display_question = first_question[:50] + "..." if len(first_question) > 50 else first_question
        
        # Format timestamp
        formatted_time = session_start.strftime("%m/%d %H:%M") if hasattr(session_start, 'strftime') else str(session_start)
        
        # Load chat button (full width)
        button_label = f"🕒 {formatted_time}\n{display_question}"
        if st.sidebar.button(
            button_label,
            key=f"load_chat_{session_id}_{i}",
            help=f"Load chat session from {formatted_time}",
            use_container_width=True
        ):
            if not st.session_state.loading_chat:
                st.session_state.loading_chat = True
                # Load the chat session
                chat_history = load_chat_session(session_id)
                
                if chat_history:
                    # Clear current messages and load the chat history
                    st.session_state.messages = [{"role": "assistant", "content": "How can I help you?"}]
                    
                    for chat_entry in chat_history:
                        # Add user message
                        st.session_state.messages.append({
                            "role": "user", 
                            "content": chat_entry['USER_QUESTION']
                        })
                        
                        # Add assistant response
                        assistant_msg = {
                            "role": "assistant", 
                            "content": chat_entry['ASSISTANT_RESPONSE']
                        }
                        
                        # Add source data if available
                        if chat_entry.get('SOURCES_USED'):
                            try:
                                import json
                                sources_data = json.loads(chat_entry['SOURCES_USED'])
                                assistant_msg["source_data"] = sources_data
                            except:
                                pass  # If JSON parsing fails, just don't include sources
                        
                        st.session_state.messages.append(assistant_msg)
                    
                    # Update session ID to the loaded one for continuity
                    st.session_state.session_id = session_id
                    st.session_state.loading_chat = False
                    st.rerun()

else:
    st.sidebar.info("No previous chats found")

# New Chat button
if st.sidebar.button("✨ New Chat", key="new_chat_button", use_container_width=True, help="Start a new chat session"):
    # Generate new session ID
    st.session_state.session_id = str(uuid.uuid4())
    # Reset messages to initial state
    st.session_state.messages = [{"role": "assistant", "content": "How can I help you?"}]
    # Clear feedback state
    st.session_state.feedback_given = {}
    st.rerun()

# Clear All chat history button
if st.sidebar.button("🗑️ Clear All Chat History", key="delete_all_chats", help="Delete all chat history", use_container_width=True):
    if delete_chat_history():
        st.success("All chat history deleted!")
        st.rerun()

# ============================================================================
# DATE FILTERING (MOVED TO TOP AND MADE FUNCTIONAL)
# ============================================================================
st.sidebar.markdown("---")
st.sidebar.subheader("📅 Document Date Filter")

# Initialize date filtering state
if "date_filter_enabled" not in st.session_state:
    st.session_state.date_filter_enabled = False

# Get the date range from the database
try:
    date_range_query = f"""
    SELECT 
        MIN(eff_code_final_date) as min_date,
        MAX(eff_code_final_date) as max_date,
        COUNT(DISTINCT eff_code_final_date) as unique_dates
    FROM {db_name}.{schema_name}.DOCS_CHUNKS_TABLE 
    WHERE eff_code_final_date IS NOT NULL
    """
    date_range_result = session.sql(date_range_query).collect()
    
    if date_range_result and date_range_result[0]['MIN_DATE'] and date_range_result[0]['MAX_DATE']:
        min_date = date_range_result[0]['MIN_DATE']
        max_date = date_range_result[0]['MAX_DATE']
        unique_dates = date_range_result[0]['UNIQUE_DATES']
        
        # Convert to datetime.date objects
        min_date_obj = min_date.date() if hasattr(min_date, 'date') else min_date
        max_date_obj = max_date.date() if hasattr(max_date, 'date') else max_date
        
        # Date filter toggle
        date_filter_enabled = st.sidebar.toggle(
            'Filter by Date Range', 
            value=st.session_state.date_filter_enabled,
            key="date_filter_toggle",
            help="Filter documents to specific date range"
        )
        
        if date_filter_enabled != st.session_state.date_filter_enabled:
            st.session_state.date_filter_enabled = date_filter_enabled
        
        if date_filter_enabled:
            # Date range selector
            col1, col2 = st.sidebar.columns(2)
            with col1:
                start_date = st.sidebar.date_input(
                    "From Date",
                    value=min_date_obj,
                    min_value=min_date_obj,
                    max_value=max_date_obj,
                    key="start_date_input"
                )
            with col2:
                end_date = st.sidebar.date_input(
                    "To Date", 
                    value=max_date_obj,
                    min_value=min_date_obj,
                    max_value=max_date_obj,
                    key="end_date_input"
                )
            
            st.sidebar.info(f"📊 Filtering to dates: {start_date} to {end_date}")
        else:
            st.sidebar.info(f"""
            **Available Document Dates:**
            📅 **Range:** {min_date_obj} to {max_date_obj}
            📊 **Unique Dates:** {unique_dates}
            """)
            # Set default values when not filtering
            start_date = min_date_obj
            end_date = max_date_obj
                  
except Exception as e:
    st.sidebar.error(f"Error fetching date range: {str(e)}")
    date_filter_enabled = False
    start_date = None
    end_date = None

# Fixed model (no user selection) and Auto-enable Cortex Search
FIXED_MODEL = 'llama3.1-70b'
cortex_search_on = True  # Always enabled
show_sources = True      # Always enabled


# Add app reset button
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Reset App", help="Clear all session data and restart", key="reset_app_button"):
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

#------------------------------------------------------------------------------
# MAIN UI SETUP
#------------------------------------------------------------------------------

# App title
st.title("📄 MassHealth Publications AI Research Assistant")

# Initialize chat messages in session state if not already set
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "How can I help you?"}]

# Initialize session ID if not exists
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

# Initialize feedback state if not exists
if "feedback_given" not in st.session_state:
    st.session_state.feedback_given = {}

# Initialize key counter for unique keys
if "key_counter" not in st.session_state:
    st.session_state.key_counter = 0

#------------------------------------------------------------------------------
# HELPER FUNCTIONS
#------------------------------------------------------------------------------

def get_unique_key(base_key):
    """Generate a unique key by incrementing counter"""
    st.session_state.key_counter += 1
    return f"{base_key}_{st.session_state.key_counter}"

def save_feedback_to_snowflake(session_id, message_index, user_question, assistant_response, feedback_type):
    """Save user feedback to Snowflake"""
    try:
        # Use parameterized query to handle special characters
        feedback_query = f"""
        INSERT INTO {db_name}.{schema_name}.CHAT_FEEDBACK 
        (session_id, message_index, user_question, assistant_response, feedback_type)
        VALUES (?, ?, ?, ?, ?)
        """
        session.sql(feedback_query, params=[session_id, message_index, user_question, assistant_response, feedback_type]).collect()
        return True
    except Exception as e:
        # Create table if it doesn't exist
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.CHAT_FEEDBACK (
                feedback_id VARCHAR DEFAULT UUID_STRING(),
                session_id VARCHAR,
                message_index INTEGER,
                user_question VARCHAR,
                assistant_response VARCHAR,
                feedback_type VARCHAR,
                feedback_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                user_id VARCHAR DEFAULT 'anonymous'
            )
            """
            session.sql(create_table_query).collect()
            # Try inserting again
            session.sql(feedback_query, params=[session_id, message_index, user_question, assistant_response, feedback_type]).collect()
            return True
        except Exception as e2:
            st.error(f"Error saving feedback: {str(e2)}")
            return False

def display_feedback_buttons(message_index, user_question, assistant_response):
    """Display thumbs up/down buttons for feedback"""
    feedback_key = f"msg_{message_index}"
    
    # Check if feedback was already given for this message
    if feedback_key in st.session_state.feedback_given:
        feedback_type = st.session_state.feedback_given[feedback_key]
        if feedback_type == 'positive':
            st.success("✅ Thank you for your positive feedback!")
        else:
            st.info("✅ Thank you for your feedback!")
        return
    
    # Display feedback buttons
    col1, col2, col3 = st.columns([1, 1, 8])
    
    with col1:
        if st.button("👍", key=get_unique_key(f"positive_{message_index}"), help="This response was helpful"):
            if save_feedback_to_snowflake(
                st.session_state.session_id, 
                message_index, 
                user_question, 
                assistant_response, 
                'positive'
            ):
                st.session_state.feedback_given[feedback_key] = 'positive'
                st.rerun()
    
    with col2:
        if st.button("👎", key=get_unique_key(f"negative_{message_index}"), help="This response was not helpful"):
            if save_feedback_to_snowflake(
                st.session_state.session_id, 
                message_index, 
                user_question, 
                assistant_response, 
                'negative'
            ):
                st.session_state.feedback_given[feedback_key] = 'negative'
                st.rerun()

def highlight_citations(text, show_sources=True):
    """
    Process text to highlight and make citation markers clickable.
    
    Args:
        text (str): The text containing citation markers like [1], [2,3], etc.
        show_sources (bool): Whether to make citations clickable links to sources
    """
    # Split text into paragraphs to preserve formatting
    paragraphs = text.split('\n')
    
    for paragraph in paragraphs:
        if not paragraph.strip():
            # Empty line, just add a line break
            st.write("")
            continue
            
        # Process each paragraph to replace citations with HTML
        processed_paragraph = ""
        parts = re.split(r'(\[\d+(?:,\s*\d+)*\])', paragraph)
        
        for part in parts:
            # If this part is a citation marker
            if re.match(r'\[\d+(?:,\s*\d+)*\]', part):
                # Extract the numbers from the citation
                citation_nums = re.findall(r'\d+', part)
                
                # Add the citation as HTML
                if show_sources:
                    # If sources are shown, make citations clickable
                    citation_html = f'<span style="color: #ff4b4b; font-weight: bold;"><a href="#source_{"_".join(citation_nums)}" style="color: #ff4b4b; text-decoration: none;">{part}</a></span>'
                else:
                    # If sources are hidden, still highlight but don't make clickable
                    citation_html = f'<span style="color: #ff4b4b; font-weight: bold;">{part}</span>'
                processed_paragraph += citation_html
            else:
                # Regular text
                if part:
                    processed_paragraph += part
        
        # Write the entire processed paragraph as a single markdown element
        st.markdown(processed_paragraph, unsafe_allow_html=True)

def display_copy_button(text_to_copy, message_index=None):
    """
    Use download button as a copy alternative
    """
    import re
    
    # Clean the text
    clean_text = re.sub(r'<[^>]+>', '', text_to_copy)
    clean_text = re.sub(r'\[(\d+(?:,\s*\d+)*)\]', r'[\1]', clean_text)
    clean_text = clean_text.replace('\\n', '\n').replace('\\\"', '"')
    clean_text = clean_text.strip()
    
    # Generate unique key
    unique_key = get_unique_key(f"download_{message_index}")
    
    # Download button - this actually works automatically
    st.download_button(
        label="📋 Download Response",
        data=clean_text,
        file_name="response.txt",
        mime="text/plain",
        key=unique_key
    )
    
def display_sources(sources, message_index=None, chunk_info=None):
    """
    Display source documents in expandable sections with download buttons inside.
    """
    if not show_sources:
        return
        
    st.markdown("---")
    
    # Show chunk selection info if available
    if chunk_info:
        st.markdown(f"### Sources (Ranked by Relevance) - {chunk_info}")
    else:
        st.markdown("### Sources (Ranked by Relevance)")
    
    for i, result in enumerate(sources):
        # Create an anchor for this source
        st.markdown(f'<div id="source_{i+1}"></div>', unsafe_allow_html=True)
        
        # Extract metadata
        relative_path = result.get('relative_path')
        if not relative_path and 'metadata' in result and result['metadata']:
            relative_path = result['metadata'].get('relative_path')

        # Extract eff_code_final_date
        eff_code_final_date = result.get('eff_code_final_date')
        if eff_code_final_date is None and 'metadata' in result and result['metadata']:
            eff_code_final_date = result['metadata'].get('eff_code_final_date')
        eff_code_display = f" | {eff_code_final_date}" if eff_code_final_date and str(eff_code_final_date).strip() else ""

        # Add relevance ranking to title
        relevance_rank = f"#{i+1} Most Relevant" if i == 0 else f"#{i+1}"
        
        # Construct title with ranking
        if relative_path:
            source_title = f"🔍 {relevance_rank} - {relative_path}{eff_code_display}"
        else:
            source_title = f"🔍 {relevance_rank} - Source{eff_code_display}"
        
        # Create the expandable section with download button inside
        with st.expander(f"{source_title}", expanded=False):
            # Create two columns - one for content, one for download button
            col1, col2 = st.columns([4, 1])
            
            with col1:
                # Display the relevant chunk
                chunk_text = result["chunk"]
                chunk_text = chunk_text.replace('\\\"\\\"', '"')
                chunk_text = chunk_text.replace('\\\"', '"')
                chunk_text = chunk_text.replace('\\n', '\n')
                
                st.text_area(
                    "Relevant excerpt:",
                    value=chunk_text,
                    height=200,
                    key=get_unique_key(f"chunk_display_{message_index}_{i}"),
                    label_visibility="visible"
                )
            
            with col2:
                # Add download button inside the expander
                if relative_path:
                    try:
                        # Get full document
                        full_doc_query = f"""
                        SELECT chunk 
                        FROM {db_name}.{schema_name}.DOCS_CHUNKS_TABLE 
                        WHERE relative_path = '{relative_path}'
                        """
                        
                        full_doc_result = session.sql(full_doc_query).collect()
                        
                        if full_doc_result:
                            # Reconstruct full document
                            full_document = "\n".join([row['CHUNK'] for row in full_doc_result])
                            full_document = full_document.replace('\\\"\\\"', '"')
                            full_document = full_document.replace('\\\"', '"')
                            full_document = full_document.replace('\\n', '\n')
                            
                            # Create safe filename
                            safe_filename = relative_path.replace('/', '_').replace('\\', '_').replace(':', '_')
                            
                            # Download button inside the expander
                            st.download_button(
                                label="📁 Download Full Document",
                                data=full_document,
                                file_name=f"{safe_filename}.txt",
                                mime="text/plain",
                                key=get_unique_key(f"download_full_{message_index}_{i}"),
                                help=f"Download the complete document: {relative_path}"
                            )
                            
                    except Exception as e:
                        st.error(f"Error preparing download for {relative_path}: {str(e)}")
                else:
                    st.caption("Download not available")

st.markdown("---")

#------------------------------------------------------------------------------
# DISPLAY CHAT HISTORY
#------------------------------------------------------------------------------

# Display all messages from history
for i, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            # Display the message with citations
            if "source_data" in message:
                highlight_citations(message["content"], show_sources)
                
                # Add download response button BEFORE sources
                if i > 0:  # Don't show button for initial greeting
                    display_copy_button(message["content"], message_index=i)
                
                # Display sources if enabled
                chunk_info = message.get("chunk_info", None)
                display_sources(message["source_data"], message_index=i, chunk_info=chunk_info)
            else:
                st.write(message["content"])
                
                # Add download response button for non-source responses too
                if i > 0:  # Don't show button for initial greeting
                    display_copy_button(message["content"], message_index=i)
            
            # Add feedback buttons for assistant messages (skip the initial greeting)
            if i > 0:  # Don't show feedback for the initial "How can I help you?" message
                # Find the corresponding user question
                user_question = ""
                if i > 0 and st.session_state.messages[i-1]["role"] == "user":
                    user_question = st.session_state.messages[i-1]["content"]
                
                display_feedback_buttons(i, user_question, message["content"])
        else:
            # Regular user message
            st.write(message["content"])

#------------------------------------------------------------------------------
# HANDLE USER INPUT
#------------------------------------------------------------------------------

# Get user input
prompt = st.chat_input("Ask a question...")

# Process the user input
if prompt:
    # Always use dynamic chunks with maximum range (3-15) for comprehensive context
    min_chunks = 3
    max_chunks = 15
    actual_num_chunks = determine_chunk_count(prompt, min_chunks, max_chunks)
    complexity_info = get_complexity_explanation(prompt)
    chunk_info_display = f"{actual_num_chunks} chunks selected dynamically"
    
    # Show complexity analysis in an info box
    with st.chat_message("assistant"):
        st.info(f"🧠 **Question Analysis:** {complexity_info}\n\n📊 **Selected {actual_num_chunks} source chunks** for this response")
    
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.write(prompt)
    
    # Build conversation history using ALL messages in current chat session
    recent_messages = st.session_state.messages[:-1]  # Exclude the current prompt
    
    # Format conversation history
    conversation_history = ""
    for message in recent_messages:
        role = "User" if message["role"] == "user" else "Assistant"
        conversation_history += f"{role}: {message['content']}\n\n"
    
    # Initialize variables for search results
    context = ""
    error_occurred = False
    
    #--------------------------------------------------------------------------
    # CORTEX SEARCH WITH FUNCTIONAL DATE FILTERING
    #--------------------------------------------------------------------------
    if cortex_search_on:
        # Build filter dictionary with date filtering only
        filter_conditions = []
        
        # Date filter (now functional)
        if date_filter_enabled and start_date and end_date:
            # Convert dates to string format for filtering
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            # Add date range filter
            filter_conditions.append({
                "@and": [
                    {"@gte": {"eff_code_final_date": start_date_str}},
                    {"@lte": {"eff_code_final_date": end_date_str}}
                ]
            })
        
        # Combine filters
        if len(filter_conditions) == 0:
            filter_dict = None
        elif len(filter_conditions) == 1:
            filter_dict = filter_conditions[0]
        else:
            filter_dict = {"@and": filter_conditions}

        # Query the Cortex Search Service
        try:
            cortex_service = root.databases[db_name].schemas[schema_name].cortex_search_services[search_service_name]
            
            # Set a high limit for comprehensive search
            SEARCH_LIMIT = 1000

            question_response = cortex_service.search(
                prompt, 
                ["chunk","relative_path", "eff_code_final_date"], 
                filter=filter_dict if filter_dict else None,
                limit=SEARCH_LIMIT
            )

            # Cortex Search returns results ranked by relevance score

            # Build context string from search results using dynamic chunk count
            for i, result in enumerate(question_response.results[:actual_num_chunks]):
                doc_title = result.get('relative_path', 'Unknown')
                eff_date = result.get('eff_code_final_date', '')
                date_display = f" (Effective Date: {eff_date})" if eff_date else ""
                context += f"Source {i+1} - {doc_title}{date_display}:\n{result['chunk']}\n\n"

        except Exception as e:
            st.error(f"An error occurred while querying Cortex Search: {str(e)}")
            error_occurred = True
    
    #--------------------------------------------------------------------------
    # BUILD SYSTEM MESSAGE
    #--------------------------------------------------------------------------
    if cortex_search_on and not error_occurred:
        # Create source previews for citation guidance
        source_list = ""
        for i, result in enumerate(question_response.results[:actual_num_chunks]):
            # Get the first 100 characters of each source as a preview
            preview = result['chunk'][:100] + "..." if len(result['chunk']) > 100 else result['chunk']
            source_list += f"Source {i+1}: {preview}\n\n"
        
        # RAG-specific system message - moved outside f-string to avoid backslash issue
        context_section = f"Context:\n{context}"
        sources_section = f"Sources:\n{source_list}"
        
        system_message = f"""You are an AI assistant specifically designed to answer questions based solely on the provided context. Your knowledge is limited to the information below.
        
{context_section}

{sources_section}

These sources are chunks from documents. Sources with the same number prefix come from the same document. The sources are already ranked by relevance, with Source 1 being the most relevant to the user's question. The sources include documents from various effective dates to provide comprehensive coverage. The number of sources provided ({actual_num_chunks}) was automatically selected based on the complexity of the question.

Instructions:
1. Carefully analyze the provided context.
2. Answer questions only based on the above information.
3. If the context lacks sufficient details, state specifically what information is missing. For example: "The provided sources mention X but don't specify Y, which would be needed to fully answer this question."
4. Maintain a professional and concise tone.
5. IMPORTANT: For each claim or piece of information in your response, add a citation marker like [1], [2], etc. that corresponds to the source number in the context. If a claim comes from multiple sources, include all relevant numbers like [1,3].
6. Only cite sources that directly support your statement. Don't cite sources that weren't used.
7. Be precise with your citations - make sure each citation points to a source that actually contains that specific information.
8. Structure your answers in this format when possible:
   - Start with a direct answer to the question
   - Provide supporting details with proper citations
   - If appropriate, include a brief summary at the end
9. When answering complex questions, briefly explain your reasoning, showing how you arrived at the answer based on the provided sources.
10. Be precise in your statements. Don't generalize beyond what the sources explicitly state. Maintain factual accuracy at all costs.
11. Each source may include additional metadata, such as "Effective Date" which is stored as yyyy-mm-dd. If the question refers to effective dates or timeframes, use the "Effective Date" value from the relevant source(s) and cite them precisely. You can also infer the effective date from the title of the document if necessary. 
12. When multiple sources with different effective dates contain relevant information, include information from all relevant sources and note the different effective dates in your response.
13. Use all {actual_num_chunks} sources effectively - this number was specifically chosen based on the complexity of the question to provide comprehensive coverage.
"""
    else:
        # General-purpose system message (when Cortex Search is off)
        system_message = """You are an advanced AI assistant designed to provide exceptional support. You have been trained on a wide range of knowledge and can answer questions across many domains.

Instructions:
1. Answer questions to the best of your abilities using your internal knowledge.
2. Express uncertainty when you don't know something rather than making up information.
3. Maintain a professional and concise tone in your responses.
4. Structure your answers in this format when possible:
   - Start with a direct answer to the question - Format so it is easy to read and understand
   - Provide supporting details and explanations if needed
   - If appropriate, include a brief summary at the end - YOU DO NOT ALWAYS HAVE TO DO THIS
5. When answering complex questions, briefly explain your reasoning to help the user understand your thought process.
6. Be precise in your statements and make clear distinctions between facts, opinions, and speculations.
7. Consider the context of the conversation when formulating your responses.
8. If the user asks for creative content like code, stories, or business ideas, feel free to be imaginative while ensuring practical usefulness.
"""
    
    #--------------------------------------------------------------------------
    # GENERATE AND DISPLAY RESPONSE
    #--------------------------------------------------------------------------
    # Combine system instructions with conversation history and the new prompt
    full_prompt = f"{system_message}\n{conversation_history}\nUser: {prompt}"
    
    # Call the Cortex complete UDF
    try:
        # Generate response using fixed model
        response_df = session.create_dataframe([full_prompt]).select(
            call_udf('snowflake.cortex.complete', FIXED_MODEL, concat(lit(full_prompt)))
        )
        full_response = response_df.collect()[0][0]
        
        # Store the response with source data if available
        response_message = {"role": "assistant", "content": full_response}
        if cortex_search_on and not error_occurred and 'question_response' in locals():
            response_message["source_data"] = question_response.results[:actual_num_chunks]
            response_message["chunk_info"] = chunk_info_display
        
        # Add response to chat history
        st.session_state.messages.append(response_message)
        
        # Save Q&A to CHAT_HISTORY table
        sources_json = None
        if cortex_search_on and not error_occurred and 'question_response' in locals():
            try:
                # Convert sources to JSON for storage
                sources_data = []
                for result in question_response.results[:actual_num_chunks]:
                    sources_data.append({
                        'chunk': result.get('chunk', ''),
                        'relative_path': result.get('relative_path', ''),
                        'eff_code_final_date': str(result.get('eff_code_final_date', ''))
                    })
                sources_json = json.dumps(sources_data)
            except:
                sources_json = None
        
        # Save the Q&A pair to the database
        save_chat_to_history(st.session_state.session_id, prompt, full_response, sources_json)
        
        # Get the message index for the new response
        new_message_index = len(st.session_state.messages) - 1
        
        # Display the response with clickable citation links
        with st.chat_message("assistant"):
            highlight_citations(full_response, show_sources)
            
            # Add download response button BEFORE sources
            display_copy_button(full_response, message_index=new_message_index)
            
            # Display sources if enabled (already ranked by relevance)
            if cortex_search_on and not error_occurred and 'question_response' in locals():
                display_sources(question_response.results[:actual_num_chunks], message_index=new_message_index, chunk_info=chunk_info_display)
                
        # Add feedback buttons for the new response
        user_question = prompt
        display_feedback_buttons(new_message_index, user_question, full_response)
                
    except Exception as e:
        st.error(f"An error occurred while processing the response: {str(e)}")
