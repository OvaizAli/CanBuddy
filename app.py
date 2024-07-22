import streamlit as st
from dotenv import load_dotenv
import pandas as pd
import requests
import os
from io import StringIO
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationalRetrievalChain
from htmltemplate import css, bot_template, user_template
from langchain_huggingface import HuggingFaceEndpoint

# Load environment variables
load_dotenv()

# Set up environment variables
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
HUGGINGFACEHUB_API_TOKEN = os.getenv("HUGGINGFACEHUB_API_TOKEN")
LAMBDA_URL = "https://ww6tb2nasltu45faaytvzx7m6u0twwxt.lambda-url.us-east-1.on.aws/"

def fetch_csv_from_lambda(url):
    """Fetch CSV content from Lambda function."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_content = response.text
        return csv_content
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching CSV file from Lambda: {str(e)}")
        return None

def get_csv_text(csv_content):
    """Convert CSV content to text."""
    try:
        df = pd.read_csv(StringIO(csv_content))
        text = df.to_string(index=False)
        return text
    except Exception as e:
        st.error(f"Error processing CSV content: {str(e)}")
        return None

def get_text_chunks(text):
    """Split text into chunks."""
    text_splitter = CharacterTextSplitter(
        separator="\n",
        chunk_size=1000,
        chunk_overlap=0,
        length_function=len
    )
    chunks = text_splitter.split_text(text)
    return chunks

def get_vectorstore(text_chunks):
    """Create a vectorstore from text chunks."""
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    if not text_chunks:
        raise ValueError("No text chunks to create vectorstore.")
    vectorstore = FAISS.from_texts(texts=text_chunks, embedding=embeddings)
    return vectorstore

def get_conversation_chain(vectorstore):
    """Create a conversation chain with a given vectorstore."""
    repo_id = "mistralai/Mistral-7B-Instruct-v0.3"
    llm = HuggingFaceEndpoint(
        repo_id=repo_id, max_length=128, temperature=0.5, token=HUGGINGFACEHUB_API_TOKEN)
    memory = ConversationBufferMemory(
        memory_key='chat_history', return_messages=True)
    conversation_chain = ConversationalRetrievalChain.from_llm(
        llm=llm,
        retriever=vectorstore.as_retriever(),
        memory=memory
    )
    return conversation_chain

def handle_userinput(user_question):
    """Handle user input and display conversation."""
    response = st.session_state.conversation({'question': user_question})
    st.session_state.chat_history = response['chat_history']

    # Start the chat container
    st.markdown('<div class="chat-container">', unsafe_allow_html=True)

    for i, message in enumerate(st.session_state.chat_history[-2:]):
        if i % 2 == 0:
            st.write(bot_template.replace(
                "{{MSG}}", message.content), unsafe_allow_html=True)
        else:
            st.write(user_template.replace(
                "{{MSG}}", message.content), unsafe_allow_html=True)

    # End the chat container
    st.markdown('</div>', unsafe_allow_html=True)

def handle_userinput_NA(user_question):
    """Handle user input and display conversation."""
    response = st.session_state.conversation({'question': user_question})
    st.session_state.chat_history = response['chat_history']

    # Start the chat container
    st.markdown('<div class="chat-container">', unsafe_allow_html=True)

    for i, message in enumerate(st.session_state.chat_history[-1:]):
        st.write(user_template.replace(
            "{{MSG}}", message.content), unsafe_allow_html=True)

    # End the chat container
    st.markdown('</div>', unsafe_allow_html=True)

def generate_article(prompt, context):
    """Generate an article based on a given prompt and context."""
    # Truncate context to fit within the token limit
    max_input_tokens = 15000  # Adjust based on the model's max token limit
    truncated_context = context[:max_input_tokens]

    response = st.session_state.conversation({'question': f"{prompt}\n\nContext:\n{truncated_context}"})
    return response.get('answer', 'No article generated.')

def main():
    load_dotenv()
    st.set_page_config(page_title="CanBuddy's Reddit LLM", page_icon=":brain:")
    st.write(css, unsafe_allow_html=True)

    # Initialize session state variables
    if "conversation" not in st.session_state:
        st.session_state.conversation = None
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "vectorstore" not in st.session_state:
        st.session_state.vectorstore = None

    # Fetch and process the CSV file on startup if vectorstore is not in session
    if st.session_state.vectorstore is None:
        with st.spinner("CanBuddy is gearing UP!"):
            csv_content = fetch_csv_from_lambda(LAMBDA_URL)
            if csv_content:
                all_text = get_csv_text(csv_content)
                
                if not all_text:
                    st.error("No text extracted from the CSV file.")
                    return
                
                text_chunks = get_text_chunks(all_text)
                if not text_chunks:
                    st.error("No text chunks created from the extracted text.")
                    return
                
                st.session_state.vectorstore = get_vectorstore(text_chunks)
                st.session_state.conversation = get_conversation_chain(st.session_state.vectorstore)
                
            else:
                st.error("Failed to fetch or process the CSV file.")
                return

    # Provide options after processing
    st.header("CanBuddy's Reddit LLM :brain:")
    option = st.selectbox("Choose an option:", ["Chat with the Assistant", "Generate a Newspaper Article"])
    
    if option == "Chat with the Assistant":
        user_question = st.text_input("Ask questions about Canada:")
        if user_question:
            handle_userinput(user_question)
    elif option == "Generate a Newspaper Article":
        user_question = "As a professional newspaper editor, you are given content for all the Canadian based Redditt posts. Use all your best knowledge to give me an engaging newspaper article mentioning all the relevant details. Remember not to mention headings like 'Title', 'Subtitle' Keep it self explanatory and do not mention the subreddit names too."
        # Generate and display the article
        with st.spinner("Generating the Article..."):
            handle_userinput_NA(user_question)

if __name__ == '__main__':
    main()
