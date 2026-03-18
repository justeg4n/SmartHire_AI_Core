import os
import re
import pandas as pd
import docx

# 1. TEXT CLEANING ENGINE
def clean_text(text):
    """Cleans raw text by removing HTML tags and extra spaces."""
    if not isinstance(text, str):
        return ""
    
    # Remove HTML tags (common in web-scraped JDs)
    text = re.sub(r'<[^>]+>', ' ', text)
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\.,;:()\-&/]', ' ', text)
    # Replace multiple spaces or newlines with a single space
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

# 2. PROCESS CVs (.docx)
def process_docx_cvs(folder_path):
    """Reads all .docx files in a folder and returns a list of cleaned texts."""
    print(f"Scanning folder: {folder_path}...")
    cv_data = []
    
    if not os.path.exists(folder_path):
        print(f"[-] Error: Folder '{folder_path}' does not exist.")
        return cv_data

    for filename in os.listdir(folder_path):
        if filename.endswith(".docx") and not filename.startswith("~$"):
            file_path = os.path.join(folder_path, filename)
            try:
                # Open DOCX and extract text from paragraphs
                doc = docx.Document(file_path)
                full_text = "\n".join([para.text for para in doc.paragraphs])
                
                # Clean the extracted text
                cleaned_cv = clean_text(full_text)
                
                cv_data.append({
                    "filename": filename,
                    "cleaned_text": cleaned_cv
                })
                print(f"  [+] Processed CV: {filename}")
            except Exception as e:
                print(f"  [-] Failed to read {filename}: {e}")
                
    return cv_data

# 3. PROCESS JDs (.csv)
def process_jd_csv(csv_path):
    """Reads the JD CSV, fills missing values, and cleans the description text."""
    print(f"\nReading JD dataset: {csv_path}...")
    
    try:
        # Load CSV using pandas
        df = pd.read_csv(csv_path)
        print(f"Original JD dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns.")
        
        # 1. Fill ALL missing values (NaN / blank spaces) with "Not Specified"
        # This fixes the issue with empty salary, date_added, organization columns.
        df = df.fillna("Not Specified")
        
        # 2. Clean the 'job_description' text
        print("Cleaning JD text data (Removing HTML and weird formatting)...")
        # Creates a new column so you keep the original data intact
        df['cleaned_job_description'] = df['job_description'].apply(clean_text)
        
        print(f"  [+] Successfully processed {len(df)} Job Descriptions.")
        return df
        
    except Exception as e:
        print(f"[-] Failed to process CSV: {e}")
        return None

# 4. EXECUTE THE PIPELINE
if __name__ == "__main__":
    # Paths based on your previous terminal outputs
    CV_FOLDER = r"D:\FPTU\AWS\AI_SmartHire\data\cv\archive\Resumes" 
    
    # If you used the Copy-Paste trick to bypass the file lock, change this to the "- Copy.csv" file
    JD_CSV_FILE = r"D:\FPTU\AWS\AI_SmartHire\data\jd\monster_com-job_sample.csv"
    
    print("=== STARTING DATA CLEANING PIPELINE ===")
    
    # 1. Run CV Processing
    cleaned_cvs = process_docx_cvs(CV_FOLDER)
    
    # 2. Run JD Processing
    cleaned_jds = process_jd_csv(JD_CSV_FILE)
    
    # 3. Save the results
    if cleaned_cvs:
        cv_df = pd.DataFrame(cleaned_cvs)
        cv_df.to_csv("cleaned_cvs.csv", index=False)
        print("\n Saved cleaned CVs to 'cleaned_cvs.csv'")
        
    if cleaned_jds is not None:
        cleaned_jds.to_csv("cleaned_jds.csv", index=False)
        print(" Saved cleaned JDs to 'cleaned_jds.csv' with ALL columns intact.")
        
    print("=== PIPELINE COMPLETE ===")