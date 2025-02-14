import streamlit as st

# Import from Home.py
from Home import get_or_create_collection, chroma_client, openai_ef

st.title("Most Redundant Chapters")
st.info("Only looks at the first 10 titles")

if st.button("Find Similar Chapters (⚠️ takes up to 1 minute)"):
    try:
        collection = get_or_create_collection()
        # Get all documents
        all_docs = collection.get()
        
        if not all_docs['ids']:
            st.warning("No documents found in the database. Please load some content first.")
        else:
            # Create a list to store similarity pairs and a set to track seen pairs
            similarity_pairs = []
            seen_pairs = set()
            
            # Compare each document with every other document
            for i in range(len(all_docs['ids'])):
                # Query using the current document
                try:
                    # Request fewer results to avoid HNSW index limitations
                    n_results = min(10, len(all_docs['ids']) - 1)  # -1 to exclude self
                    if n_results <= 0:
                        continue
                        
                    results = collection.query(
                        query_texts=[all_docs['documents'][i]],
                        n_results=n_results,
                        include=['documents', 'metadatas', 'distances']
                    )
                except Exception as e:
                    print(f"Error querying document {i}: {str(e)}")
                    continue
                
                # Process results
                for j in range(len(results['distances'][0])):
                    doc1_meta = all_docs['metadatas'][i]
                    doc2_meta = results['metadatas'][0][j]
                    
                    # Only consider pairs from different titles
                    if doc1_meta['title'] == doc2_meta['title']:
                        continue
                    
                    # Create a unique pair identifier that's the same regardless of order
                    pair_key = tuple(sorted([
                        f"{doc1_meta['title']}_{doc1_meta['chapter']}",
                        f"{doc2_meta['title']}_{doc2_meta['chapter']}"
                    ]))
                    
                    # Skip if we've seen this pair before
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)
                    
                    # Convert distance to similarity
                    distance = results['distances'][0][j]
                    similarity = (2 - distance) / 2  # Maps distance [0,2] to similarity [1,0]
                    
                    similarity_pairs.append({
                        'pair_id': pair_key,
                        'similarity': similarity,
                        'doc1_meta': doc1_meta,
                        'doc2_meta': doc2_meta,
                        'doc1_content': all_docs['documents'][i],
                        'doc2_content': results['documents'][0][j]
                    })
            
            # Sort pairs by similarity (higher similarity means more similar)
            similarity_pairs.sort(key=lambda x: x['similarity'], reverse=True)
            
            # Display top 5 most similar pairs
            st.subheader("Top 5 Most Similar Chapter Pairs")
            
            # Create tabs for each pair
            top_pairs = similarity_pairs[:5]
            tabs = st.tabs([f"Pair {i+1} (Similarity: {pair['similarity']:.2%})" for i, pair in enumerate(top_pairs)])
            
            for i, (tab, pair) in enumerate(zip(tabs, top_pairs)):
                with tab:
                    # Container for the pair
                    with st.container():
                        # Create two columns for the documents
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown(f"**Title {pair['doc1_meta']['title']}, Chapter {pair['doc1_meta']['chapter']}**")
                            st.markdown(f"Word count: {pair['doc1_meta']['word_count']}")
                            st.markdown("### Content:")
                            st.markdown(pair['doc1_content'][:2000] + "..." if len(pair['doc1_content']) > 2000 else pair['doc1_content'])
                            
                        with col2:
                            st.markdown(f"**Title {pair['doc2_meta']['title']}, Chapter {pair['doc2_meta']['chapter']}**")
                            st.markdown(f"Word count: {pair['doc2_meta']['word_count']}")
                            st.markdown("### Content:")
                            st.markdown(pair['doc2_content'][:2000] + "..." if len(pair['doc2_content']) > 2000 else pair['doc2_content'])
                        


    except Exception as e:
        st.error(f"Error finding similar documents: {str(e)}")
