# eCFR Agency Word Count Analyzer

This Streamlit application analyzes word counts from the Electronic Code of Federal Regulations (eCFR) API by agency.

## Features
- Select different agencies from the eCFR
- View word counts per title within each agency
- Interactive bar charts using Plotly
- Raw data display
- Total word count metrics

## Setup
1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Run the Streamlit app:
```bash
streamlit run app.py
```

## Usage
1. Select an agency from the dropdown menu
2. The app will fetch and display:
   - Bar chart of word counts by title
   - Raw data table
   - Total word count for the agency
