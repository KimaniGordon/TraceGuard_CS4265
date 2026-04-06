import happybase

def reset_table():
    try:
        connection = happybase.Connection('localhost', port=9090)
        table_name = 'threat_intel'
        
        # 1. Check if it exists and delete it
        if table_name.encode() in connection.tables():
            print(f"[INFO] Deleting old table '{table_name}'...")
            connection.delete_table(table_name, disable=True)
        
        # 2. Create the table with the 'cf' Column Family
        print(f"[INFO] Creating fresh table '{table_name}' with family 'cf'...")
        connection.create_table(table_name, {'cf': dict()})
        
        print("[SUCCESS] HBase is now ready for the 'cf' mapping!")
        connection.close()
    except Exception as e:
        print(f"[ERROR] Reset failed: {e}")

if __name__ == "__main__":
    reset_table()