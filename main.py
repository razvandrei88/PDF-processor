import os
import subprocess
import fnmatch
import csv
import argparse
import concurrent.futures

def get_pdf_info(file_path):
    try:
        # Execute pdfinfo and capture the output
        output = subprocess.check_output(['pdfinfo', file_path], stderr=subprocess.DEVNULL, text=True)
        pages = None
        size = None
        title = None
        author = None

        # Process the output to extract pages, size, title, and author
        for line in output.splitlines():
            if "Pages:" in line:
                pages = int(line.split(":")[1].strip())
            elif "File size:" in line:
                size = int(line.split(":")[1].strip().split()[0])  # Extract only the number
            elif "Title:" in line:
                title = line.split(":")[1].strip()
            elif "Author:" in line:
                author = line.split(":")[1].strip()

        return pages, size, title, author
    except subprocess.CalledProcessError as e:
        print(f"Error processing file '{file_path}': {e}")
        return None, None, None, None
    except Exception as e:
        print(f"An unexpected error occurred with file '{file_path}': {e}")
        return None, None, None, None

# Function to process a single PDF file
def process_pdf(file_path):
    print(f"Processing: {file_path}")
    pages, size, title, author = get_pdf_info(file_path)
    if pages is not None and size is not None:
        ratio = size / pages if pages > 0 else 0
        return [author, title, pages, size, ratio, file_path]
    return None

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Extract PDF metadata and save to CSV.')
    parser.add_argument('-d', '--directory', type=str, default='.', 
                        help='Directory to scan for PDF files (default: current directory)')
    parser.add_argument('-o', '--output', type=str, default='pdf_info.csv', 
                        help='Output CSV file name (default: pdf_info.csv)')
    
    args = parser.parse_args()

    results = []
    pdf_files = []

    # Collect all PDF files from the specified directory
    for dirpath, _, filenames in os.walk(args.directory):
        for filename in fnmatch.filter(filenames, '*.pdf'):
            pdf_files.append(os.path.join(dirpath, filename))

    total_files = len(pdf_files)
    print(f"Found {total_files} PDF files to process.")

    # Use ThreadPoolExecutor to process files in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(process_pdf, file_path): file_path for file_path in pdf_files}
        for future in concurrent.futures.as_completed(future_to_file):
            result = future.result()
            if result:
                results.append(result)

    # Export results to the specified CSV file
    with open(args.output, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Author', 'Title', 'Pages', 'Size (bytes)', 'Ratio', 'File Path'])  # Header
        csv_writer.writerows(results)

    print(f"Processing complete. Results saved to '{args.output}'.")

if __name__ == "__main__":
    main()