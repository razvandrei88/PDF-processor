import os
import subprocess
import fnmatch
import csv

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


def main():
    results = []
    for dirpath, _, filenames in os.walk('.'):
        for filename in fnmatch.filter(filenames, '*.pdf'):
            file_path = os.path.join(dirpath, filename)
            pages, size, title, author = get_pdf_info(file_path)

            if pages is not None and size is not None:
                ratio = size / pages if pages > 0 else 0
                results.append([author, title, pages, size, ratio, file_path])

    # Exportăm rezultatele în CSV
    with open('pdf_info.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Author', 'Title', 'Pages', 'Size (bytes)', 'Ratio', 'File Path'])  # Header
        csv_writer.writerows(results)

if __name__ == "__main__":
    main()