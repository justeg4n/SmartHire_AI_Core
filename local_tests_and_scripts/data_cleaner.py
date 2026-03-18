import os
import re
import pandas as pd
import docx

# ==========================================
# 1. THE ULTIMATE TEXT CLEANING ENGINE
# ==========================================
def clean_text(text):
    """Obliterates HTML, invisible spaces, Â symbols, and bad encoding."""
    if pd.isna(text) or not isinstance(text, str):
        return "Not Specified"
    
    # 1. Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # 2. Rescue common punctuation before nuking
    text = text.replace('“', '"').replace('”', '"').replace("‘", "'").replace("’", "'")
    text = text.replace("–", "-").replace("—", "-")
    
    # 3. THE NUCLEAR OPTION: Force to strict ASCII.
    # This physically deletes Â, \xa0, and ANY other weird web symbol.
    text = text.encode('ascii', 'ignore').decode('ascii')
    
    # 4. Remove leftover weird punctuation but keep the basics
    text = re.sub(r'[^\w\s\.,;:()\-&/]', ' ', text)
    
    # 5. Compress multiple spaces/newlines into a single space
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

# ==========================================
# 2. PROCESS CVs (.docx)
# ==========================================
def process_docx_cvs(folder_path):
    print(f"Scanning folder: {folder_path}...")
    cv_data = []
    
    if not os.path.exists(folder_path):
        print(f"[-] Error: Folder '{folder_path}' does not exist.")
        return cv_data

    for filename in os.listdir(folder_path):
        if filename.endswith(".docx") and not filename.startswith("~$"):
            file_path = os.path.join(folder_path, filename)
            try:
                doc = docx.Document(file_path)
                full_text = "\n".join([para.text for para in doc.paragraphs])
                cleaned_cv = clean_text(full_text)
                cv_data.append({"filename": filename, "cleaned_text": cleaned_cv})
                print(f"  [+] Processed CV: {filename}")
            except Exception as e:
                print(f"  [-] Failed to read {filename}: {e}")
                
    return cv_data

# ==========================================
# 3. PROCESS JDs (.csv)
# ==========================================
def process_jd_csv(csv_path):
    print(f"\nReading JD dataset: {csv_path}...")
    try:
        # Load CSV (Ignoring bad lines if any exist in the web scrape)
        df = pd.read_csv(csv_path, on_bad_lines='skip')
        print(f"Original JD dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns.")
        
        # 1. Fill ALL missing values
        df = df.fillna("Not Specified")
        
        # 2. OVERWRITE the original columns directly to avoid confusion
        print("Cleaning JD text data (Nuking Â symbols and HTML)...")
        df['job_description'] = df['job_description'].apply(clean_text)
        
        # We also clean the job_title just in case it has Â symbols
        df['job_title'] = df['job_title'].apply(clean_text)
        
        print(f"  [+] Successfully processed {len(df)} Job Descriptions.")
        return df
        
    except Exception as e:
        print(f"[-] Failed to process CSV: {e}")
        return None

# ==========================================
# 4. EXECUTE THE PIPELINE
# ==========================================
if __name__ == "__main__":
    # Ensure these paths are correct for your machine
    CV_FOLDER = r"D:\FPTU\AWS\AI_SmartHire\data\cv\archive\Resumes" 
    
    # If you used the Copy-Paste trick earlier, make sure this points to the right file
    JD_CSV_FILE = r"D:\FPTU\AWS\AI_SmartHire\data\jd\monster_com-job_sample.csv"
    
    print("=== STARTING DATA CLEANING PIPELINE ===")
    
    cleaned_cvs = process_docx_cvs(CV_FOLDER)
    cleaned_jds = process_jd_csv(JD_CSV_FILE)
    
    # SAVING WITH 'utf-8-sig' STOPS EXCEL FROM CORRUPTING THE VIEW
    if cleaned_cvs:
        cv_df = pd.DataFrame(cleaned_cvs)
        cv_df.to_csv("cleaned_cvs.csv", index=False, encoding='utf-8-sig')
        print("\n Saved cleaned CVs to 'cleaned_cvs.csv'")
        
    if cleaned_jds is not None:
        cleaned_jds.to_csv("cleaned_jds.csv", index=False, encoding='utf-8-sig')
        print(" Saved cleaned JDs to 'cleaned_jds.csv'.")
        
    print("=== PIPELINE COMPLETE ===")