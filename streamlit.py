import streamlit as st
import json
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete

def main():
    st.title("Cortex Search in Streamlit")

    # Initialize Snowflake session
    session = get_active_session()

    # Input box for user query
    query = st.text_input("Enter your search query:", "")

    # Perform the search when the user clicks the button
    if st.button("Search"):
        with st.spinner("Searching..."):
            try:
                # Construct the JSON payload for the SQL query safely
                search_payload = json.dumps({
                    "query": query,
                    "columns": ["combined_text"],
                    "limit": 5
                })

                # Safely construct the SQL query
                sql_query = f"""
                SELECT PARSE_JSON(
                  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                      'marvel.services.marvel_search_service',
                      '{search_payload}'
                  )
                )['results'] AS results
                """

                # Execute the query
                query_result = session.sql(sql_query).collect()

                # Parse results from the collected query output
                if query_result:
                    # Extract the first row's "results" field and parse as JSON
                    results_json = json.loads(query_result[0]["RESULTS"])

                    if results_json:
                        st.success("Results Found:")

                        context = ""
                        for res in results_json:
                            document_text = res['combined_text']
                            st.write(f"**Document Content:** {document_text}")
                            st.write("---")
                            # Add the document text to the context string
                            context += f"Document: {document_text}\n\n"
                            
                        # Construct the prompt with the gathered context
                        prompt = f"Given the following documents, answer the question: {query}\n\n{context}"

                        # Query the model with the complete context
                        completion = Complete(model="mistral-large2", prompt=prompt)

                        # Display the result from the model
                        st.write("Mistral's Answer: ", completion)
                        st.write("---")
                    else:
                        st.warning("No results found.")
                else:
                    st.warning("No results found.")
            except Exception as e:
                st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
