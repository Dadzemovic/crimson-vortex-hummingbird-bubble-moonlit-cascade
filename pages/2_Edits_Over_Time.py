import streamlit as st
import duckdb
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

st.title("Content Versions Over Time")

try:
    # Query to get cumulative versions over time for each title
    versions_query = """
    WITH RECURSIVE date_series AS (
        -- Generate all dates between min and max dates
        SELECT MIN(date) - INTERVAL '1 day' as date FROM content_versions
        UNION ALL
        SELECT date + INTERVAL '1 day'
        FROM date_series
        WHERE date < (SELECT MAX(date) FROM content_versions)
    ),
    title_dates AS (
        -- Cross join titles with all dates
        SELECT 
            t.number as title,
            t.name as title_name,
            ds.date
        FROM titles t
        CROSS JOIN date_series ds
    ),
    daily_versions AS (
        SELECT 
            t.number as title,
            t.name as title_name,
            cv.date,
            COUNT(*) as daily_versions,
            cv.substantive
        FROM titles t
        JOIN content_versions cv ON t.number = cv.title
        GROUP BY t.number, t.name, cv.date, cv.substantive
    ),
    all_versions AS (
        SELECT 
            td.title,
            td.title_name,
            td.date,
            COALESCE(SUM(dv.daily_versions), 0) as daily_total
        FROM title_dates td
        LEFT JOIN daily_versions dv ON 
            td.title = dv.title AND 
            td.date = dv.date
        GROUP BY td.title, td.title_name, td.date
    ),
    substantive_versions AS (
        SELECT 
            td.title,
            td.title_name,
            td.date,
            COALESCE(SUM(CASE WHEN dv.substantive THEN dv.daily_versions ELSE 0 END), 0) as daily_substantive
        FROM title_dates td
        LEFT JOIN daily_versions dv ON 
            td.title = dv.title AND 
            td.date = dv.date
        GROUP BY td.title, td.title_name, td.date
    )
    SELECT 
        title,
        title_name,
        date,
        SUM(daily_total) OVER (
            PARTITION BY title
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as total_versions,
        SUM(daily_substantive) OVER (
            PARTITION BY title
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as substantive_versions
    FROM (
        SELECT 
            av.title,
            av.title_name,
            av.date,
            av.daily_total,
            sv.daily_substantive
        FROM all_versions av
        JOIN substantive_versions sv ON 
            av.title = sv.title AND 
            av.date = sv.date
    )
    ORDER BY title, date;
    """
    
    versions_df = con.execute(versions_query).df()
    
    if not versions_df.empty:
        # Create two tabs for the different views
        tab1, tab2 = st.tabs(["All Versions", "Substantive Changes Only"])
        
        with tab1:
            # Create line plot for all versions
            all_versions_fig = px.line(
                versions_df,
                x='date',
                y='total_versions',
                color='title',
                hover_data=['title'],
                title='Cumulative Content Versions Over Time by Title',
                labels={
                    'date': 'Date',
                    'total_versions': 'Total Versions',
                    'title': 'Title'
                }
            )
            
            all_versions_fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Cumulative Number of Versions",
                hovermode='x unified',
                xaxis=dict(showgrid=True),
                yaxis=dict(showgrid=True, rangemode='nonnegative'),
                plot_bgcolor='white',
                height=600,
                showlegend=True
            )
            
            # Update line styling
            all_versions_fig.update_traces(
                mode='lines',
                line=dict(shape='hv')
            )
            
            st.plotly_chart(all_versions_fig, use_container_width=True)
            
        
        with tab2:
            # Create line plot for substantive changes only
            substantive_fig = px.line(
                versions_df,
                x='date',
                y='substantive_versions',
                color='title',
                hover_data=['title'],
                title='Cumulative Substantive Changes Over Time by Title',
                labels={
                    'date': 'Date',
                    'substantive_versions': 'Total Substantive Changes',
                    'title': 'Title'
                }
            )
            
            substantive_fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Cumulative Number of Substantive Changes",
                hovermode='x unified',
                xaxis=dict(showgrid=True),
                yaxis=dict(showgrid=True, rangemode='nonnegative'),
                plot_bgcolor='white',
                height=600,
                showlegend=True
            )
            
            # Update line styling
            substantive_fig.update_traces(
                mode='lines',
                line=dict(shape='hv')
            )
            
            st.plotly_chart(substantive_fig, use_container_width=True)
            
            # Show raw data in expandable section
            with st.expander("View Raw Data"):
                st.dataframe(versions_df[['date', 'title', 'substantive_versions']])
    else:
        st.warning("No version data available")
        
except Exception as e:
    st.error(f"Error generating content versions visualization: {str(e)}")
