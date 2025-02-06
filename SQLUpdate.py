import pandas as pd
import pyodbc
import numpy as np  # Import NumPy to check for NaN

def connect_to_db(host, user, password, database):
    try:
        conn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={host};'
            f'DATABASE={database};'
            f'UID={user};'
            f'PWD={password};'
            'TrustServerCertificate=yes;'
        )
        print("✅ Connected to database successfully!")
        return conn
    except Exception as e:
        print("❌ Error connecting to database:", e)
        return None

def update_column_from_csv(conn, table_name, column_name, csv_file, old_col_name, new_col_name):
    try:
        df = pd.read_csv(csv_file)
        df.columns = df.columns.str.strip()  # Remove spaces from column names
        print("📋 CSV Columns:", df.columns)

        if old_col_name not in df.columns or new_col_name not in df.columns:
            print(f"❌ CSV file must contain '{old_col_name}' and '{new_col_name}' columns.")
            return

        cursor = conn.cursor()

        for _, row in df.iterrows():
            old_value = str(row[old_col_name]).strip()
            new_value = row[new_col_name]  # Do not convert to string yet

            # 🔍 Check for NaN or empty values
            if pd.isna(new_value) or new_value == "":
                print(f"⚠️ Skipping update for '{old_value}' as new value ('{new_value}') is NaN or empty.")
                continue  # Skip this row

            new_value = str(new_value).strip()  # Convert after NaN check

            update_query = f"""
            UPDATE {table_name}
            SET {column_name} = ? 
            WHERE {column_name} = ?
            """
            print(f"🔄 Updating: {old_value} → {new_value}")
            cursor.execute(update_query, (new_value, old_value))

        conn.commit()
        print("✅ Database updated successfully!")
    except Exception as e:
        print("❌ Error updating database:", e)
    finally:
        cursor.close()

# Example usage
if __name__ == "__main__":
    HOST = "totalancillary.database.windows.net"
    USER = "DBRW"
    PASSWORD = "JjaT/?(N44v6"
    DATABASE = "TA-DEV" #"AXIS-API-DEV" #"TA-Dev"

    db_connection = connect_to_db(HOST, USER, PASSWORD, DATABASE)

    if db_connection:
        update_column_from_csv(
            db_connection, 
            "v_RepAccountAR", 
            "Customer", 
            "HV360_Clinics.csv",#"HV360_Physicians.csv", #"HV360_Clinics.csv", 
            "Original Name",  
            "Updated Name"  
        )
        db_connection.close()
