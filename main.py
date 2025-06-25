import os
import subprocess
import fnmatch

def get_pdf_info(file_path):
    try:
        # Executăm pdfinfo și capturăm ieșirea
        output = subprocess.check_output(['pdfinfo', file_path], stderr=subprocess.DEVNULL, text=True)
        pages = None
        size = None

        # Procesăm ieșirea pentru a extrage numărul de pagini și dimensiunea
        for line in output.splitlines():
            if "Pages:" in line:
                pages = int(line.split(":")[1].strip())
            elif "File size:" in line:
                size = int(line.split(":")[1].strip().split()[0])  # Extragem doar numărul

        return pages, size
    except Exception as e:
        return None, None

def main():
    for dirpath, _, filenames in os.walk('.'):
        for filename in fnmatch.filter(filenames, '*.pdf'):
            file_path = os.path.join(dirpath, filename)
            pages, size = get_pdf_info(file_path)

            if pages is not None and size is not None:
                ratio = size / pages if pages > 0 else 0
                print(f"File: {file_path}, Pages: {pages}, Size: {size} bytes, Ratio: {ratio:.2f}")

if __name__ == "__main__":
    main()
