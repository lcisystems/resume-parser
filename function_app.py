import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import requests
import csv
import spacy
import re
import nltk
import json
import io
import tempfile

from spacy.matcher import Matcher
nltk.download('stopwords')
nltk.download('punkt')
from io import BytesIO
from pdfminer.high_level import extract_text, extract_text_to_fp
import os
import hashlib

app = func.FunctionApp()

def serialize_sets(obj):
    if isinstance(obj, set):
        return list(obj)

    return obj

@app.route(route="upload_resume", auth_level=func.AuthLevel.ANONYMOUS)
def upload(req: func.HttpRequest) -> func.HttpResponse:
    email = req.params.get('email')  # Assuming the email is passed as a query parameter
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)

    logging.info('Received a request to upload a file.')

    # Retrieve the connection string
    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf"  # Assuming PDF format

    container_name = 'resumeparser'  # Replace with your container name

    file_bytes = req.get_body()
    if not file_bytes:
        return func.HttpResponse("No file uploaded", status_code=400)

    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(file_bytes, overwrite=True)

        file_url = blob_client.url
    
        output = {
            "message" : "uploaded successfully",
            "Access URL": f"{file_url}",
        }
        return func.HttpResponse(json.dumps(output), status_code=200,
        mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Failed to upload file. Exception: {str(e)}", status_code=500)


@app.route(route="get_resume", auth_level=func.AuthLevel.ANONYMOUS)
def get_resume(req: func.HttpRequest) -> func.HttpResponse:
    email = req.params.get('email')
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)
    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf" 

    logging.info('Received a request to read a file.')

    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    container_name = 'resumeparser'  # Replace with your actual container name
    # Expecting the blob name as a query parameter

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        return func.HttpResponse(body=blob_content, status_code=200, mimetype="application/pdf")
    except Exception as e:
        return func.HttpResponse(f"Failed to read file. Exception: {str(e)}", status_code=500)



@app.route(route="resume_parser", auth_level=func.AuthLevel.ANONYMOUS)
def main(req: func.HttpRequest) -> func.HttpResponse:
    
    email = req.params.get('email')
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)
    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf" 
    
    logging.info('Received a request to read a file.')

    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    container_name = 'resumeparser'  # Replace with your actual container name
    # Expecting the blob name as a query parameters

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    
    
    
    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        
        temp_dir = tempfile.gettempdir()
        output_file_path = os.path.join(temp_dir, 'output.txt')

        # Assuming blob_content contains your PDF data
        pdf_stream = io.BytesIO(blob_content)
        with open(output_file_path, 'w', encoding='utf-8') as out_file:
            extract_text_to_fp(pdf_stream, outfp=out_file)

        # Now you can read from the file as needed
        with open(output_file_path, 'r', encoding='utf-8') as file:
            text = file.read().lower()
        
        
        # # Extract text from PDF
        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     text=extract_text_to_fp(pdf_stream, outfp=out_file)

        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     extract_text_to_fp(pdf_stream, outfp=out_file)
        # f = open("output.txt", "r", encoding='utf-8')
        # text = f.read().lower()
        # Reset the stream position to the start if you need to read the stream again
        pdf_stream.seek(0)
        
        # skills = skills_db()
        
        skills = extract_skills(text)
        name = extract_name(text)
        contact_number = extract_contact_number_from_resume(text)
        email = extract_email_from_resume(text)
        education = extract_education_from_resume(text)
        
        output = {
            "email": email,
            "phone": contact_number,
            "name": name,
            "skills": skills,
            "education": education
        }
        
        body = json.dumps(output)
        
        # Close the stream
        pdf_stream.close()
        return func.HttpResponse(body=body, status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Failed to read file. Exception: {str(e)}", status_code=500)



@app.route(route="skills", auth_level=func.AuthLevel.ANONYMOUS)
def get_skills(req: func.HttpRequest) -> func.HttpResponse:
    
    email = req.params.get('email')
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)
    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf" 
    
    logging.info('Received a request to read a file.')

    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    container_name = 'resumeparser'  # Replace with your actual container name
    # Expecting the blob name as a query parameter

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    
    
    
    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        temp_dir = tempfile.gettempdir()
        output_file_path = os.path.join(temp_dir, 'output.txt')

        # Assuming blob_content contains your PDF data
        pdf_stream = io.BytesIO(blob_content)
        with open(output_file_path, 'w', encoding='utf-8') as out_file:
            extract_text_to_fp(pdf_stream, outfp=out_file)

        # Now you can read from the file as needed
        with open(output_file_path, 'r', encoding='utf-8') as file:
            text = file.read().lower()
        
        
        # # Extract text from PDF
        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     text=extract_text_to_fp(pdf_stream, outfp=out_file)

        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     extract_text_to_fp(pdf_stream, outfp=out_file)
        # f = open("output.txt", "r", encoding='utf-8')
        # text = f.read().lower()
        # Reset the stream position to the start if you need to read the stream again
        pdf_stream.seek(0)
        
        # skills = skills_db()
        
        skills = extract_skills(text)
        
        output = {
            "skills": skills,
        }
        
        body = json.dumps(output)
        
        # Close the stream
        pdf_stream.close()
        return func.HttpResponse(body=body, status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Failed to read file. Exception: {str(e)}", status_code=500)



@app.route(route="educaton", auth_level=func.AuthLevel.ANONYMOUS)
def get_education(req: func.HttpRequest) -> func.HttpResponse:
    
    email = req.params.get('email')
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)
    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf" 
    
    logging.info('Received a request to read a file.')

    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    container_name = 'resumeparser'  # Replace with your actual container name
    # Expecting the blob name as a query parameter

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    
    
    
    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        
        temp_dir = tempfile.gettempdir()
        output_file_path = os.path.join(temp_dir, 'output.txt')

        # Assuming blob_content contains your PDF data
        pdf_stream = io.BytesIO(blob_content)
        with open(output_file_path, 'w', encoding='utf-8') as out_file:
            extract_text_to_fp(pdf_stream, outfp=out_file)

        # Now you can read from the file as needed
        with open(output_file_path, 'r', encoding='utf-8') as file:
            text = file.read().lower()
        pdf_stream.seek(0)
        education = extract_education_from_resume(text)
        output = {
            "education": education,
        }
        
        body = json.dumps(output)
        
        # Close the stream
        pdf_stream.close()
        return func.HttpResponse(body=body, status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Failed to read file. Exception: {str(e)}", status_code=500)


@app.route(route="contact", auth_level=func.AuthLevel.ANONYMOUS)
def get_contact(req: func.HttpRequest) -> func.HttpResponse:
    
    email = req.params.get('email')
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)
    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf" 
    
    logging.info('Received a request to read a file.')

    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    container_name = 'resumeparser'  # Replace with your actual container name
    # Expecting the blob name as a query parameter

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    
    
    
    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        # Extract text from PDF
        temp_dir = tempfile.gettempdir()
        output_file_path = os.path.join(temp_dir, 'output.txt')

        # Assuming blob_content contains your PDF data
        pdf_stream = io.BytesIO(blob_content)
        with open(output_file_path, 'w', encoding='utf-8') as out_file:
            extract_text_to_fp(pdf_stream, outfp=out_file)

        # Now you can read from the file as needed
        with open(output_file_path, 'r', encoding='utf-8') as file:
            text = file.read().lower()
        
        pdf_stream.seek(0)
            
        contact_number = extract_contact_number_from_resume(text)
       
        
        output = {
            "phone": contact_number
        }
        
        body = json.dumps(output)
        
        # Close the stream
        pdf_stream.close()
        return func.HttpResponse(body=body, status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Failed to read file. Exception: {str(e)}", status_code=500)
    
@app.route(route="email", auth_level=func.AuthLevel.ANONYMOUS)
def get_email(req: func.HttpRequest) -> func.HttpResponse:
    
    email = req.params.get('email')
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)
    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf" 
    
    logging.info('Received a request to read a file.')

    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    container_name = 'resumeparser'  # Replace with your actual container name
    # Expecting the blob name as a query parameter

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    
    
    
    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        
        temp_dir = tempfile.gettempdir()
        output_file_path = os.path.join(temp_dir, 'output.txt')

        # Assuming blob_content contains your PDF data
        pdf_stream = io.BytesIO(blob_content)
        with open(output_file_path, 'w', encoding='utf-8') as out_file:
            extract_text_to_fp(pdf_stream, outfp=out_file)

        # Now you can read from the file as needed
        with open(output_file_path, 'r', encoding='utf-8') as file:
            text = file.read().lower()
        
        
        # # Extract text from PDF
        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     text=extract_text_to_fp(pdf_stream, outfp=out_file)

        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     extract_text_to_fp(pdf_stream, outfp=out_file)
        # f = open("output.txt", "r", encoding='utf-8')
        # text = f.read().lower()
        # Reset the stream position to the start if you need to read the stream again
        pdf_stream.seek(0)
        email = extract_email_from_resume(text)
        output = {
            "email": email
        }
        
        body = json.dumps(output)
        
        # Close the stream
        pdf_stream.close()
        return func.HttpResponse(body=body, status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Failed to read file. Exception: {str(e)}", status_code=500)

@app.route(route="demographic_info", auth_level=func.AuthLevel.ANONYMOUS)
def get_demographic_info(req: func.HttpRequest) -> func.HttpResponse:
    
    email = req.params.get('email')
    if not email:
        return func.HttpResponse("Please provide an email address.", status_code=400)
    email_hash = hashlib.sha256(email.encode()).hexdigest()
    file_name = f"resumes/{email_hash}.pdf" 
    
    logging.info('Received a request to read a file.')

    connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    container_name = 'resumeparser'  # Replace with your actual container name
    # Expecting the blob name as a query parameter

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    
    
    
    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        # Extract text from PDF
        
        temp_dir = tempfile.gettempdir()
        output_file_path = os.path.join(temp_dir, 'output.txt')

        # Assuming blob_content contains your PDF data
        pdf_stream = io.BytesIO(blob_content)
        with open(output_file_path, 'w', encoding='utf-8') as out_file:
            extract_text_to_fp(pdf_stream, outfp=out_file)

        # Now you can read from the file as needed
        with open(output_file_path, 'r', encoding='utf-8') as file:
            text = file.read().lower()
        
        
        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     text=extract_text_to_fp(pdf_stream, outfp=out_file)

        # pdf_stream = io.BytesIO(blob_content)
        # with open('output.txt', 'w', encoding='utf-8') as out_file:
        #     extract_text_to_fp(pdf_stream, outfp=out_file)
        # f = open("output.txt", "r", encoding='utf-8')
        # text = f.read().lower()
        # Reset the stream position to the start if you need to read the stream again
        pdf_stream.seek(0)
        email = extract_email_from_resume(text)
        output = {
            "email": email
        }
        
        body = json.dumps(output)
        
        # Close the stream
        pdf_stream.close()
        return func.HttpResponse(body=body, status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Failed to read file. Exception: {str(e)}", status_code=500)
        
def extract_skills(input_text):
    
    nlp = spacy.load('en_core_web_lg')
    skills = "jz_skills_pattern.jsonl"
    ruler = nlp.add_pipe("entity_ruler", before='ner')
    ruler.from_disk(skills)
    
    doc = nlp(input_text)
    dict = {}
    skills = []
    i = 0
    for ent in doc.ents:
        if ent.label_=='SKILL':
            skills.append(ent.text)
            
    skills = [i.capitalize() for i in set([i.lower() for i in skills])]
    dict = skills
    return dict
    

def extract_education_from_resume(text):
    education = []

    # Use regex pattern to find education information
    #pattern = #r"(?i)(?:Bsc|\bB\.\w+|\bM\.\w+|\bPh\.D\.\w+|\bBachelor(?:'s)?|\bMaster(?:'s)?|\bPh\.D)\s(?:\w+\s)*\w+"
    pattern = r"(?i)(?:Bsc(?:\s\(Hons\))?|\bB\.\w+|\bM\.\w+|\bPh\.D\.\w+|\bBachelor(?:'s)?|\bMaster(?:'s)?|\bPh\.D)\s(?:\w+\s)*\w+"
    matches = re.findall(pattern, text)
    for match in matches:
        education.append(match.strip())

    return education

def extract_name(resume_text):
    nlp = spacy.load('en_core_web_sm')
    matcher = Matcher(nlp.vocab)

    # Define name patterns
    patterns = [
        [{'POS': 'PROPN'}, {'POS': 'PROPN'}],  # First name and Last name
        [{'POS': 'PROPN'}, {'POS': 'PROPN'}, {'POS': 'PROPN'}],  # First name, Middle name, and Last name
        [{'POS': 'PROPN'}, {'POS': 'PROPN'}, {'POS': 'PROPN'}, {'POS': 'PROPN'}]  # First name, Middle name, Middle name, and Last name
        # Add more patterns as needed
    ]

    for pattern in patterns:
        matcher.add('NAME', patterns=[pattern])

    doc = nlp(resume_text)
    matches = matcher(doc)

    for match_id, start, end in matches:
        span = doc[start:end]
        return span.text

    return None

def extract_contact_number_from_resume(text):
    contact_number = None

    # Use regex pattern to find a potential contact number
    pattern = r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    match = re.search(pattern, text)
    if match:
        contact_number = match.group()

    return contact_number

def extract_email_from_resume(text):
    email = None

    # Use regex pattern to find a potential email address
    pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
    match = re.search(pattern, text)
    if match:
        email = match.group()

    return email

def skills_db():
    # load pre-trained model
    base_path = os.path.dirname(__file__)
    
    file = os.path.join(base_path,"LINKEDIN_SKILLS_ORIGINAL.txt")
    # Initialize an empty list to store the skills
    skills_list = []

    # Open the CSV file and read each row into the list
    with open(file, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            # Assuming each skill is in the first column
            skills_list.append(row[0].lower())

    # Print the list of skills
    return skills_list


def extract_education_from_resume(text):
    education = []

    # Use regex pattern to find education information
    #pattern = #r"(?i)(?:Bsc|\bB\.\w+|\bM\.\w+|\bPh\.D\.\w+|\bBachelor(?:'s)?|\bMaster(?:'s)?|\bPh\.D)\s(?:\w+\s)*\w+"
    pattern = r"(?i)(?:Bsc(?:\s\(Hons\))?|\bB\.\w+|\bM\.\w+|\bPh\.D\.\w+|\bBachelor(?:'s)?|\bMaster(?:'s)?|\bPh\.D)\s(?:\w+\s)*\w+"
    matches = re.findall(pattern, text)
    for match in matches:
        education.append(match.strip())

    return education
 


