import streamlit as st
import boto3
import duckdb
import json
import openai
import requests
from tenacity import retry, wait_random_exponential, stop_after_attempt
from termcolor import colored
import altair as alt
from timeit import default_timer as timer

GPT_MODEL = "gpt-3.5-turbo-0613"

def get_openai_api_key():
    # Connect to the AWS Systems Manager service
    ssm = boto3.client('ssm')
    # Retrieve the OpenAI API key from AWS Parameter Store
    response = ssm.get_parameter(
        Name='openAI_api_key',
        WithDecryption=True  # Set to False if the parameter value is not encrypted
    )
    # Extract the value of the parameter
    api_key = response['Parameter']['Value']
    
    return api_key

#chatgpt utilities
@retry(wait=wait_random_exponential(min=1, max=40), stop=stop_after_attempt(3))
def chat_completion_request(messages, functions=None, function_call=None, model=GPT_MODEL):
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + openai.api_key,
    }
    json_data = {"model": model, "messages": messages}
    if functions is not None:
        json_data.update({"functions": functions})
    if function_call is not None:
        json_data.update({"function_call": function_call})
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=json_data,
        )
        return response
    except Exception as e:
        print("Unable to generate ChatCompletion response")
        print(f"Exception: {e}")
        return e

def pretty_print_conversation(messages):
    role_to_color = {
        "system": "red",
        "user": "green",
        "assistant": "blue",
        "function": "magenta",
    }
    formatted_messages = []
    for message in messages:
        if message["role"] == "system":
            formatted_messages.append(f"system: {message['content']}\n")
        elif message["role"] == "user":
            formatted_messages.append(f"user: {message['content']}\n")
        elif message["role"] == "assistant" and message.get("function_call"):
            formatted_messages.append(f"assistant: {message['function_call']}\n")
        elif message["role"] == "assistant" and not message.get("function_call"):
            formatted_messages.append(f"assistant: {message['content']}\n")
        elif message["role"] == "function":
            formatted_messages.append(f"function ({message['name']}): {message['content']}\n")
    for formatted_message in formatted_messages:
        print(
            colored(
                formatted_message,
                role_to_color[messages[formatted_messages.index(formatted_message)]["role"]],
            )
        )
    return formatted_message

def get_table_names(conn):
    """Return a list of column names."""    
    table_names= []
    tables = conn.execute("SELECT table_name FROM information_schema.tables")
    for table in tables.fetchall():
        table_names.append(table[0])
    return table_names

def get_column_names(conn, table_name):
    """Return a list of column names."""    
    column_names = []
    columns = conn.execute("SELECT column_name from information_schema.columns")
    for column in columns.fetchall():
        column_names.append(column[0])
    return column_names

def get_database_info(conn):
    """Return a list of dicts containing the table name and columns for each table in the database."""
    table_dicts = []
    for table_name in get_table_names(conn):
        columns_names = get_column_names(conn, table_name)
        table_dicts.append({"table_name": table_name, "column_names": columns_names})
    return table_dicts

def ask_database(conn, query):
    """Function to query duckdb database with a provided SQL query."""
    try:
        results = str(conn.execute(query).fetchall())
    except Exception as e:
        results = f"query failed with error: {e}"
    return results

def execute_function_call(message):
    if message["function_call"]["name"] == "ask_database":
        query = json.loads(message["function_call"]["arguments"])["query"]
        results = ask_database(conn, query)
    else:
        results = f"Error: function {message['function_call']['name']} does not exist"
    return results


#con = duckdb.connect(database='itineraries.duckdb')

openai.api_key = get_openai_api_key()
start_timer = timer()
conn = duckdb.connect(database='itineraries.duckdb', read_only=True)
database_schema_dict = get_database_info(conn)
database_schema_string = "\n".join(
    [
        f"Table: {table['table_name']}\nColumns: {', '.join(table['column_names'])}"
        for table in database_schema_dict
    ]
)


functions = [
    {
        "name": "ask_database",
        "description": "Use this function to answer user questions about flight itineraries. Output should be a fully formed SQL query.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": f"""
                            SQL query extracting info to answer the user's question.
                            SQL should be written using this database schema:
                            {database_schema_string}
                            The query should be returned in plain text, not in JSON.
                            """,
                }
            },
            "required": ["query"],
        },
    }
]

messages = []
messages.append({"role": "system", "content": "Answer user questions by generating SQL queries against the itineraries database."})

st.set_page_config(layout="wide")
st.title('Expedia flight data - Non Stop flights only')
st.subheader('Send a Message')
user_message = st.text_input(label="Input a Message")

messages.append({"role": "user", "content": user_message})
chat_response = chat_completion_request(messages, functions)
assistant_message = chat_response.json()["choices"][0]["message"]
messages.append(assistant_message)
if assistant_message.get("function_call"):
    results = execute_function_call(assistant_message)
    messages.append({"role": "function", "name": assistant_message["function_call"]["name"], "content": results})
st.write(pretty_print_conversation(messages))