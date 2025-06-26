import os
import subprocess
import fnmatch
import argparse
import concurrent.futures
import logging
import warnings
import sqlite3

# Suppress specific warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Set up logging
logging.basicConfig(filename='pdf_processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def setup_database(db_path='pdf_metadata.db'):
    """Initialize the database and create the necessary tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pdf_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author TEXT,
            title TEXT,
            pages INTEGER,
            size_bytes INTEGER,
            size_per_page_ratio REAL,
            file_path TEXT UNIQUE,
            last_processed TIMESTAMP
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_path ON pdf_metadata (file_path)')
    
    conn.commit()
    conn.close()
    logging.info(f"Database initialized at {db_path}")

def get_pdf_info(file_path):
    try:
        output = subprocess.check_output(['pdfinfo', file_path], stderr=subprocess.DEVNULL, text=True)
        pages = size = title = author = None

        for line in output.splitlines():
            if "Pages:" in line:
                pages = int(line.split(":")[1].strip())
            elif "File size:" in line:
                size = int(line.split(":")[1].strip().split()[0])
            elif "Title:" in line:
                title = line.split(":")[1].strip()
            elif "Author:" in line:
                author = line.split(":")[1].strip()

        return pages, size, title, author
    except subprocess.CalledProcessError as e:
        logging.error(f"Error processing file '{file_path}': {e}")
        return None, None, None, None
    except Exception as e:
        logging.error(f"Unexpected error with file '{file_path}': {e}")
        return None, None, None, None

def sanitize_data(author, title):
    author = author.strip() if author else "N/A"
    title = title.strip() if title else "N/A"
    
    author = ''.join(c for c in author if c.isalnum() or c in (" ", ".", ",", "-"))
    title = ''.join(c for c in title if c.isalnum() or c in (" ", ".", ",", "-"))
    
    if ',' in author:
        parts = author.split(',')
        author = f"{parts[1].strip()} {parts[0].strip()}" if len(parts) == 2 else author
    
    return author, title

def process_pdf(file_path):
    """Process a single PDF and save results to database."""
    logging.info(f"Processing: {file_path}")
    pages, size, title, author = get_pdf_info(file_path)
    
    if pages is None or size is None:
        return
    
    ratio = size / pages if pages > 0 else 0
    author, title = sanitize_data(author, title)
    
    conn = sqlite3.connect('pdf_metadata.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO pdf_metadata 
            (author, title, pages, size_bytes, size_per_page_ratio, file_path, last_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (author, title, pages, size, ratio, file_path, datetime.now()))
        
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to save {file_path}: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Extract PDF metadata and save to SQLite database.')
    parser.add_argument('-d', '--directory', type=str, default='.', 
                       help='Directory to scan for PDF files (default: current directory)')
    parser.add_argument('-db', '--database', type=str, default='pdf_metadata.db',
                       help='SQLite database file (default: pdf_metadata.db)')
    
    args = parser.parse_args()
    
    setup_database(args.database)
    
    pdf_files = []
    for dirpath, _, filenames in os.walk(args.directory):
        for filename in fnmatch.filter(filenames, '*.pdf'):
            pdf_files.append(os.path.join(dirpath, filename))
    
    total_files = len(pdf_files)
    logging.info(f"Found {total_files} PDF files to process.")
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_pdf, file_path): file_path for file_path in pdf_files}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            progress = (i + 1) / total_files
            bar = '#' * int(40 * progress) + '-' * (40 - int(40 * progress))
            print(f"\r[{bar}] {i+1}/{total_files}", end='')
    
    print("\nProcessing complete!")
    logging.info(f"Processed {total_files} files. Data saved to {args.database}")

if __name__ == "__main__":
    main()
