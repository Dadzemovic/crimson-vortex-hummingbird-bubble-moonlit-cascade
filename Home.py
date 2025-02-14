import streamlit as st
from pathlib import Path
import duckdb
import chromadb
from chromadb.utils import embedding_functions
import os
import requests
import xml.etree.ElementTree as ET
import re

# API endpoints
VERSIONER_API = "https://www.ecfr.gov/api/versioner/v1"
ADMIN_API = "https://www.ecfr.gov/api/admin/v1"

# Create data directory if it doesn't exist
data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

# Initialize ChromaDB
chroma_client = chromadb.PersistentClient(
    path="./data/chroma_db",
    settings=chromadb.Settings(
        anonymized_telemetry=False,
        allow_reset=True,
        is_persistent=True
    )
)


# Create OpenAI embedding function
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
    api_key=os.getenv('OPENAI_API_KEY'),
    model_name="text-embedding-ada-002",  # Using ada-002 for consistent 1536 dimensions
)

def initialize_database():
    """Initialize the database tables if they don't exist"""
    
    # Create tables with updated schema
    con.execute("""
        CREATE TABLE IF NOT EXISTS titles (
            number INTEGER,
            name VARCHAR,
            latest_amended_on DATE,
            latest_issue_date DATE,
            up_to_date_as_of DATE,
            reserved BOOLEAN
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS word_counts (
            title INTEGER,
            part VARCHAR,
            chapter VARCHAR,
            date DATE,
            word_count INTEGER
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS agencies (
            name VARCHAR,
            short_name VARCHAR,
            display_name VARCHAR,
            sortable_name VARCHAR,
            slug VARCHAR
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS agency_cfr_references (
            agency_name VARCHAR,
            title INTEGER,
            chapter VARCHAR
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS content_versions (
            title INTEGER,
            part VARCHAR,
            identifier VARCHAR,
            name VARCHAR,
            date DATE,
            amendment_date DATE,
            issue_date DATE,
            substantive BOOLEAN,
            removed BOOLEAN,
            subpart VARCHAR,
            type VARCHAR
        )
    """)
    
    # Commit the changes
    con.commit()


def get_or_create_collection():
    """Get or create collection"""
    try:
        collection = chroma_client.get_collection(
            name="ecfr_content",
            embedding_function=openai_ef,
        )
    except ValueError:
        collection = chroma_client.create_collection(
            name="ecfr_content",
            embedding_function=openai_ef,
        )
    return collection

def fetch_titles():
    """Fetch all titles from the versioner API"""
    response = requests.get(
        f"{VERSIONER_API}/titles.json",
        headers={"accept": "application/json"}
    )
    if response.status_code == 200:
        return response.json()["titles"]
    return None

def fetch_agencies():
    """Fetch all agencies from the admin API"""
    response = requests.get(
        f"{ADMIN_API}/agencies.json",
        headers={"accept": "application/json"}
    )
    if response.status_code == 200:
        return response.json()["agencies"]
    return None

def fetch_title_structure(title_number, date):
    """Fetch structure for a specific title"""
    response = requests.get(
        f"{VERSIONER_API}/structure/{date}/title-{title_number}.json",
        headers={"accept": "application/json"}
    )
    if response.status_code == 200:
        return response.json()
    return None

def fetch_title_versions(title_number):
    """Fetch version data for a specific title"""
    response = requests.get(
        f"{VERSIONER_API}/versions/title-{title_number}.json",
        headers={"accept": "application/json"}
    )
    if response.status_code == 200:
        return response.json()
    return None

def fetch_xml_content(title_number, date):
    """Fetch XML content for a title and optionally a specific part"""
    url = f"{VERSIONER_API}/full/{date}/title-{title_number}.xml"
    response = requests.get(url, headers={"accept": "application/xml"})
    if response.status_code == 200:
        return response.text
    return None

def chunk_text(text, max_chunk_size=6000):
    """Split text into chunks of approximately max_chunk_size tokens.
    Using a conservative estimate of 4 characters per token."""
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0
    
    for word in words:
        word_length = len(word)
        if current_length + word_length > max_chunk_size * 4:  # Approximate token count
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_length = word_length
        else:
            current_chunk.append(word)
            current_length += word_length + 1  # +1 for space
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

def count_words_in_xml(xml_content, title_number=None):
    """Count words in XML content after stripping tags, broken down by chapter"""
    from lxml import etree
    if not xml_content:
        return {}
    
    # Parse XML and get text content
    try:
        root = etree.fromstring(xml_content.encode('utf-8'))
        
        # Find all chapter elements
        chapters = root.xpath('//DIV3[@TYPE="CHAPTER"]')
        chapter_word_counts = {}
        
        for chapter in chapters:
            # Get chapter number
            chapter_num = chapter.get('N')
            
            # Get all text content within this chapter, normalized
            text = ' '.join(chapter.xpath('.//text()')).strip()
            word_count = len(text.split())
            
            # Store in dict with chapter number as key
            chapter_word_counts[chapter_num] = word_count
            
            # Store chapter text in ChromaDB only for first 10 titles
            if title_number and int(title_number) <= 10:
                try:
                    # Get ChromaDB collection
                    collection = get_or_create_collection()
                    
                    # Create metadata
                    base_metadata = {
                        "title": str(title_number),
                        "chapter": str(chapter_num),
                        "word_count": word_count,
                        "type": "chapter_text"
                    }
                    
                    # Clean and normalize the text
                    cleaned_text = ' '.join(text.split())  # Remove extra whitespace
                    if len(cleaned_text) > 100:  # Only store if there's meaningful content
                        # Split into chunks if necessary
                        text_chunks = chunk_text(cleaned_text)
                        
                        for i, chunk in enumerate(text_chunks):
                            # Add chunk number to metadata and ID if text was chunked
                            metadata = base_metadata.copy()
                            if len(text_chunks) > 1:
                                metadata['chunk'] = i + 1
                                metadata['total_chunks'] = len(text_chunks)
                            
                            doc_id = f"title_{title_number}_chapter_{chapter_num}"
                            if len(text_chunks) > 1:
                                doc_id += f"_chunk_{i+1}"
                            
                            collection.add(
                                documents=[chunk],
                                metadatas=[metadata],
                                ids=[doc_id]
                            )
                        print(f"Stored chapter {chapter_num} in ChromaDB (length: {len(cleaned_text)} chars)")
                    else:
                        print(f"Skipping chapter {chapter_num} - too short ({len(cleaned_text)} chars)")
                except Exception as e:
                    print(f"Error storing chapter {chapter_num} in ChromaDB: {str(e)}")
        
        # Also get total word count
        total_text = ' '.join(root.xpath('//text()')).strip()
        chapter_word_counts['total'] = len(total_text.split())
        
        return chapter_word_counts
    except Exception as e:
        st.error(f"Error parsing XML: {str(e)}")
        return {}

def load_data():
    """Load data from the API into DuckDB"""
    
    try:
        # Clear ChromaDB collection first
        collection = get_or_create_collection()
        all_docs = collection.get()
        if all_docs['ids']:
            st.write(f"Deleting {len(all_docs['ids'])} existing documents from ChromaDB...")
            collection.delete(ids=all_docs['ids'])
            st.write("ChromaDB collection cleared")
        
        # Start transaction
        con.execute("BEGIN TRANSACTION")
        print("Starting data load...")
        
        # Clear existing data
        print("Clearing existing data...")
        con.execute("DELETE FROM titles")
        con.execute("DELETE FROM word_counts")
        con.execute("DELETE FROM agencies")
        con.execute("DELETE FROM agency_cfr_references")
        con.execute("DELETE FROM content_versions")
        
        # First, fetch and insert agencies
        with st.spinner("Fetching and processing agencies..."):
            agencies = fetch_agencies()
            st.write(f"Fetched {len(agencies) if agencies else 0} agencies")
            if not agencies:
                st.error("Failed to fetch agencies")
                con.execute("ROLLBACK")
                return
            
            # Insert agencies and their CFR references
            for agency in agencies:
                # Insert agency
                name = agency['name'].replace("'", "''")
                short_name = (agency.get('short_name') or '').replace("'", "''")
                display_name = (agency.get('display_name') or '').replace("'", "''")
                sortable_name = (agency.get('sortable_name') or '').replace("'", "''")
                slug = (agency.get('slug') or '').replace("'", "''")

                query = (
                    f"INSERT INTO agencies VALUES ("
                    f"'{name}', '{short_name}', '{display_name}', "
                    f"'{sortable_name}', '{slug}')"
                )
                con.execute(query)
                
                # Insert CFR references
                for ref in agency.get('cfr_references', []):
                    title = int(ref.get('title'))
                    chapter = (ref.get('chapter') or '').replace("'", "''")
                    
                    query = (
                        f"INSERT INTO agency_cfr_references VALUES ("
                        f"'{name}', {title}, '{chapter}')"
                    )
                    con.execute(query)
    
        # Then fetch and process titles
        with st.spinner("Fetching and processing titles..."):
            titles = fetch_titles()
            st.write(f"Fetched {len(titles) if titles else 0} titles")
            if not titles:
                st.error("Failed to fetch titles")
                con.execute("ROLLBACK")
                return
            
            # Process each title
            for title in titles:
                title_number = title['number']
                latest_date = title.get('latest_issue_date')
                
                # Insert title
                con.execute("""
                    INSERT INTO titles (number, name, latest_amended_on, latest_issue_date, up_to_date_as_of, reserved)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    title_number,
                    title['name'],
                    title.get('latest_amended_on'),
                    latest_date,
                    title.get('up_to_date_as_of'),
                    title.get('reserved', False)
                ])
                                        
                # Fetch and count words for entire title at once
                with st.spinner(f"Counting words for Title {title_number}..."):
                    full_xml_content = fetch_xml_content(title_number, date=latest_date)
                    if full_xml_content:
                        chapter_word_counts = count_words_in_xml(full_xml_content, title_number=title_number)
                        
                        # Store word count for each chapter
                        for chapter_num, word_count in chapter_word_counts.items():
                            if chapter_num != 'total':
                                con.execute("""
                                    INSERT INTO word_counts (title, part, date, word_count, chapter)
                                    VALUES (?, NULL, ?, ?, ?)
                                """, [title_number, latest_date, word_count, f"Chapter {chapter_num}"])
                            else:
                                # Store total word count with NULL chapter
                                con.execute("""
                                    INSERT INTO word_counts (title, part, date, word_count, chapter)
                                    VALUES (?, NULL, ?, ?, NULL)
                                """, [title_number, latest_date, word_count])
        
                # Fetch and store version data for each title
                with st.spinner("Fetching version data for all titles..."):
                    version_data = fetch_title_versions(title_number)
                    if version_data and 'content_versions' in version_data:
                        for version in version_data['content_versions']:
                            # Insert version data with proper SQL escaping
                            name = version.get('name', '').replace("'", "''")
                            subpart = (version.get('subpart') or '').replace("'", "''")
                            con.execute("""
                                INSERT INTO content_versions 
                                (title, part, identifier, name, date, amendment_date, issue_date, 
                                substantive, removed, subpart, type)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [
                                title_number,
                                version.get('part'),
                                version.get('identifier'),
                                name,
                                version.get('date'),
                                version.get('amendment_date'),
                                version.get('issue_date'),
                                version.get('substantive'),
                                version.get('removed'),
                                subpart,
                                version.get('type')
                            ])
                    else:
                        print(f"No version data found for title {title_number}")

        # Commit the transaction
        con.execute("COMMIT")
        
        # Final count check
        st.write("Final table counts:")
        for table in ['titles', 'word_counts', 'agencies', 'agency_cfr_references', 'content_versions']:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            st.write(f"{table}: {count} rows")
        
    except Exception as e:
        # If anything fails, rollback
        print("Error occurred, rolling back...")
        con.execute("ROLLBACK")
        st.error(f"Error loading data: {str(e)}")
        print(f"Error details: {str(e)}")

# Initialize DuckDB with a persistent connection
db_path = data_dir / "ecfr.duckdb"

def get_connection():
    """Get a persistent database connection"""
    conn = duckdb.connect(str(db_path), read_only=False)
    conn.execute("PRAGMA enable_verification")
    return conn

con = get_connection()

# Set up the page
st.set_page_config(
    page_title="eCFR Analysis",
    page_icon="📚",
)

st.write("# eCFR Analysis Tool 📚")

st.sidebar.success("Select a section above to begin analysis.")

st.markdown(
    """
    Welcome to the eCFR Analysis Tool! This application helps you analyze the Electronic Code of Federal Regulations (eCFR) in various ways:

    ### Available Analysis Tools
    - **📊 Word Counts**: Analyze word counts by chapter/title and agency
    - **📈 Revision History**: Track changes and versions over time
    - **🔍 Redundancy Analysis**: Find redundant sections of regulation

    ### Getting Started
    1. (Optional) Load or refresh the data using the button below
    2. Select an analysis tool from the sidebar
    3. Explore the visualizations and insights

    ### Data Sources
    This tool uses data from the eCFR API and maintains a local database for analysis.
    """
)

# Add Load Data button
if st.button("🔄 Load/Refresh Data (slow)"):
    initialize_database()
    load_data()
