import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# Get database connection
def get_connection():
    """Get a persistent database connection"""
    data_dir = Path("data")
    db_path = data_dir / "ecfr.duckdb"
    conn = duckdb.connect(str(db_path), read_only=False)
    conn.execute("PRAGMA enable_verification")
    return conn

con = get_connection()

st.title("Word Counts by Chapter")

try:
    # First show total words by title
    total_query = """
        SELECT 
            t.number as title_number,
            t.name as title_name,
            wc.word_count as total_words
        FROM titles t
        LEFT JOIN word_counts wc ON t.number = wc.title
        WHERE wc.chapter IS NULL
        ORDER BY t.number
    """
    
    total_df = pd.read_sql_query(total_query, con)
    if not total_df.empty:
        st.write("Total Words by Title:")
        # Format numbers
        total_df['total_words'] = total_df['total_words'].apply(lambda x: f"{x:,}" if x else "0")
        st.dataframe(total_df)
    
    # Then show breakdown by chapter
    chapter_query = """
        SELECT 
            t.number as title_number,
            t.name as title_name,
            wc.chapter,
            wc.word_count as word_count
        FROM titles t
        JOIN word_counts wc ON t.number = wc.title
        WHERE wc.chapter IS NOT NULL
        ORDER BY t.number, wc.chapter
    """
    
    chapter_df = pd.read_sql_query(chapter_query, con)
    if not chapter_df.empty:
        # Create a stacked bar chart showing chapter distribution for each title
        fig = px.bar(chapter_df, 
                     x='title_number', 
                     y='word_count',
                     color='chapter',
                     title='Word Count Distribution by Chapter Across Titles',
                     labels={
                         'title_number': 'Title Number',
                         'word_count': 'Total Words',
                         'chapter': 'Chapter'
                     },
                     barmode='stack'  # Stack the bars
                    )
        
        # Customize layout
        fig.update_layout(
            xaxis_title="Title Number",
            yaxis_title="Total Words",
            showlegend=True,
            legend_title="Chapter",
            hovermode='x unified',
            height=600
        )
        
        # Format hover text to show chapter and word count
        # Create custom hover text
        hover_text = []
        for _, row in chapter_df.iterrows():
            chapter_num = row['chapter'].replace('Chapter ', '') if row['chapter'] else 'Unknown'
            hover_text.append(f"Chapter {chapter_num}")
            
        fig.update_traces(
            hovertemplate="%{customdata}<br>Words: %{y:,.0f}<br>Title: %{x}<extra></extra>",
            customdata=hover_text
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    # Show word counts by agency
    st.write("\n### Word Counts by Agency")
    agency_query = """
        WITH agency_chapters AS (
            -- Get all chapters associated with each agency
            SELECT DISTINCT
                a.name as agency_name,
                a.display_name,
                acr.title,
                acr.chapter
            FROM agencies a
            JOIN agency_cfr_references acr ON a.name = acr.agency_name
        ),
        agency_word_counts AS (
            -- Sum word counts for each agency's chapters
            SELECT 
                ac.agency_name,
                ac.display_name,
                SUM(wc.word_count) as total_words
            FROM agency_chapters ac
            JOIN word_counts wc ON 
                ac.title = wc.title AND
                ac.chapter = REPLACE(wc.chapter, 'Chapter ', '')
            WHERE wc.chapter IS NOT NULL
            GROUP BY ac.agency_name, ac.display_name
        )
        SELECT 
            COALESCE(display_name, agency_name) as agency,
            total_words
        FROM agency_word_counts
        ORDER BY total_words DESC
    """
    
    agency_df = pd.read_sql_query(agency_query, con)
    if not agency_df.empty:
        # Format numbers
        agency_df['total_words'] = agency_df['total_words'].apply(lambda x: f"{x:,}" if x else "0")
        
        # Create bar chart for agency word counts
        fig = px.bar(agency_df,
                    x='agency',
                    y='total_words',
                    title='Word Counts by Agency',
                    labels={'agency': 'Agency',
                            'total_words': 'Total Words'})
        fig.update_layout(
            xaxis_tickangle=-45,
            height=600,
            margin=dict(b=150)  # increase bottom margin for rotated labels
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
except Exception as e:
    st.error(f"Error displaying word counts: {str(e)}")
