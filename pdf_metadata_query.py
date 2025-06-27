import sqlite3

def connect_db(db_path='pdf_metadata.db'):
    """Connect to the SQLite database."""
    return sqlite3.connect(db_path)

def get_largest_files(limit=10, db_path='pdf_metadata.db'):
    """Return the largest PDFs by size."""
    conn = connect_db(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM pdf_metadata ORDER BY size_bytes DESC LIMIT ?
    ''', (limit,))
    
    results = cursor.fetchall()
    conn.close()
    
    return results

def get_best_ratio(limit=10, db_path='pdf_metadata.db'):
    """Return the largest PDFs by size."""
    conn = connect_db(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM pdf_metadata ORDER BY size_per_page_ratio DESC LIMIT ?
    ''', (limit,))
    
    results = cursor.fetchall()
    conn.close()
    
    return results

def list_all_entries(db_path='pdf_metadata.db'):
    """List all entries in the database."""
    conn = connect_db(db_path)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM pdf_metadata')
    results = cursor.fetchall()
    conn.close()
    
    return results

def print_results(results):
    """Print the results in a readable format."""
    for row in results:
        print(f"ID: {row[0]}, Pages: {row[1]}, "
              f"Size (bytes): {row[2]}, Ratio: {row[3]}, File Path: {row[4]}, "
              f"Last Processed: {row[5]}")

def main():
    while True:
        print("\nPDF Metadata Query Tool")
        print("1. Get Best Ratio Files")
        print("2. Get Largest Files")
        print("3. List All Entries")
        print("4. Exit")
        
        choice = input("Select an option: ")
        
        if choice == '1':
            limit = int(input("Enter the number of best ratio files to retrieve: "))
            results = get_largest_files(limit)
            print_results(results)
        
        elif choice == '2':
            limit = int(input("Enter the number of largest files to retrieve: "))
            results = get_largest_files(limit)
            print_results(results)
        
        elif choice == '3':
            results = list_all_entries()
            print_results(results)
        
        elif choice == '4':
            print("Exiting...")
            break
        
        else:
            print("Invalid option. Please try again.")

if __name__ == "__main__":
    main()
