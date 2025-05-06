import duckdb
import os

TARGET_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/target"))
RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw"))
os.makedirs(TARGET_DIR, exist_ok=True)


def generate_unique_id(val):
    # Deterministic unique 16-char ID based on input value (for demo, not cryptographically secure)
    # You could use a hash or uuid here for real use cases
    import hashlib

    # Create a deterministic hash from the input value
    val_str = str(val) if val is not None else ""
    hash_obj = hashlib.md5(val_str.encode())
    hash_hex = hash_obj.hexdigest()

    # Use the first 16 characters of the hash
    return hash_hex[:16]


def register_udfs(con):
    con.create_function("generate_unique_id", generate_unique_id, ["VARCHAR"], "VARCHAR")


def map_patients(con):
    # Create patients table without email and phone, but with CAVE information
    query = f"""
    CREATE OR REPLACE TABLE target_patients AS
    SELECT
        generate_unique_id(CAST(p.id AS VARCHAR)) AS id,
        p.id AS patient_id,
        p.name AS first_name,
        p.surname,
        p.dob,
        CASE 
            WHEN p.gender = 1 THEN 'MALE'
            WHEN p.gender = 2 THEN 'FEMALE'
            ELSE 'UNKNOWN'
        END AS gender,
        COALESCE(c.cave, 'No information') AS cave
    FROM '{RAW_DIR}/patients.parquet' p
    LEFT JOIN '{RAW_DIR}/patients_cave.parquet' c ON p.id = c.patient_id
    """
    con.execute(query)
    con.execute(f"COPY target_patients TO '{TARGET_DIR}/patients.parquet' (FORMAT 'parquet')")
    print(f"Written mapped patients to {TARGET_DIR}/patients.parquet")

    # Create patients_com table with email and phone in separate rows
    query = f"""
    CREATE OR REPLACE TABLE target_patients_com AS
    WITH emails AS (
        SELECT
            generate_unique_id(CONCAT(CAST(id AS VARCHAR), '-email')) AS id,
            id AS patient_id,
            'EMAIL' AS com_type,
            email AS value
        FROM '{RAW_DIR}/patients.parquet'
        WHERE email IS NOT NULL
    ),
    phones AS (
        SELECT
            generate_unique_id(CONCAT(CAST(id AS VARCHAR), '-phone')) AS id,
            id AS patient_id,
            'PHONE' AS com_type,
            phone AS value
        FROM '{RAW_DIR}/patients.parquet'
        WHERE phone IS NOT NULL
    )
    SELECT * FROM emails
    UNION ALL
    SELECT * FROM phones
    """
    con.execute(query)
    con.execute(f"COPY target_patients_com TO '{TARGET_DIR}/patients_com.parquet' (FORMAT 'parquet')")
    print(f"Written patient communication data to {TARGET_DIR}/patients_com.parquet")


def map_addresses(con):
    query = f"""
    CREATE OR REPLACE TABLE target_addresses AS
    SELECT
        generate_unique_id(CAST(ROW_NUMBER() OVER () AS VARCHAR)) AS id,
        patient_id,
        address,
        city,
        postal_code,
        country
    FROM '{RAW_DIR}/addresses.parquet'
    """
    con.execute(query)
    con.execute(f"COPY target_addresses TO '{TARGET_DIR}/addresses.parquet' (FORMAT 'parquet')")
    print(f"Written mapped addresses to {TARGET_DIR}/addresses.parquet")


def main():
    con = duckdb.connect()
    register_udfs(con)
    map_patients(con)
    map_addresses(con)
    con.close()


if __name__ == "__main__":
    main()
