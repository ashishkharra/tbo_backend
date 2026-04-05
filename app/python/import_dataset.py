import psycopg2
import csv
import sys
from io import StringIO
from datetime import datetime
sys.stdout.reconfigure(encoding='utf-8')
def clean_text_from_excel(value, default=None):
    """ 
    Forces input to string. Handles Excel integers/floats: 
    - 12 becomes "12"
    - 12.0 becomes "12"
    - None/Empty becomes default
    """
    if value is None:
        return default
    s = str(value).strip()
    if s == "":
        return default
    if s.endswith(".0"):
        s = s[:-2]
    return s
def clean_date(value, default=None):
    """ Parses date strings into YYYY-MM-DD format. """
    if not value or str(value).strip() == "":
        return default
    try:
        return datetime.strptime(value.strip(), "%d-%b-%Y").strftime("%Y-%m-%d")
    except:
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            return default
def safe_int(value, default=0):
    try:
        if value is None: return default
        return int(float(str(value).strip()))
    except:
        return default
if len(sys.argv) < 3:
    print("Usage: python script.py <csv_file> <history_id>")
    sys.exit(1)
csv_file = sys.argv[1]
history_id = sys.argv[2]
debug_log = open("import_debug.log", "a", encoding="utf-8")
def log_debug(msg):
    debug_log.write(f"{datetime.now()}: {msg}\n")
try:
    conn = psycopg2.connect(
        dbname="tbo_data",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    conn.set_client_encoding("UTF8")
    cur = conn.cursor()
except Exception as e:
    print(f"Connection failed: {e}")
    sys.exit(1)
REQUIRED_COLUMNS = [
    "family_id", "vname", "fname", "mname", "dob", "sex",
    "phone1", "phone2", "distt", "block_city", "gp_ward",
    "ru", "village", "address", "cast_cat", "cast_name",
    "religion", "cast_id", "ac_no", "pc_no"
]
buffer = StringIO()
writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
writer.writerow(REQUIRED_COLUMNS)
total = 0
failed = 0
with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    reader.fieldnames = [h.strip().lower().replace(" ", "_") for h in reader.fieldnames]
    for row_index, row in enumerate(reader, start=1):
        ac_no = clean_text_from_excel(row.get("ac_no"), default="0") 
        pc_no = clean_text_from_excel(row.get("pc_no"), default="0")     
        ru = safe_int(row.get("ru"), default=0)
        dob = clean_date(row.get("dob"))
        sex = clean_text_from_excel(row.get("sex"))
        try:
            writer.writerow([
                clean_text_from_excel(row.get("family_id")),
                clean_text_from_excel(row.get("vname")),
                clean_text_from_excel(row.get("fname")),
                clean_text_from_excel(row.get("mname")),
                dob,
                sex,
                clean_text_from_excel(row.get("phone1")),
                clean_text_from_excel(row.get("phone2")),
                clean_text_from_excel(row.get("distt")),
                clean_text_from_excel(row.get("block_city")),
                clean_text_from_excel(row.get("gp_ward")),
                ru,
                clean_text_from_excel(row.get("village")),
                clean_text_from_excel(row.get("address")),
                clean_text_from_excel(row.get("cast_cat")),
                clean_text_from_excel(row.get("cast_name")),
                clean_text_from_excel(row.get("religion")),
                clean_text_from_excel(row.get("cast_id")),
                ac_no,
                pc_no
            ])
            total += 1
        except Exception as e:
            failed += 1
buffer.seek(0)
try:
    columns_str = ", ".join(REQUIRED_COLUMNS)
    cur.copy_expert(
        f"""
        COPY db_table ({columns_str})
        FROM STDIN WITH (FORMAT csv, HEADER true)
        """,
        buffer
    )
    inserted_count = cur.rowcount 
    if inserted_count == -1:
        inserted_count = total 
    cur.execute(
        "UPDATE import_history SET total_records=%s, imported_records=%s, failed_records=%s, status=%s WHERE id=%s",
        (total + failed, inserted_count, failed, "completed", history_id)
    )
    conn.commit()

    cur.execute("""
        INSERT INTO public.db_mappingmaster (
            distt,
            block_city,
            gp_ward,
            village,
            ru,
            ac_no,
            pc_no
        )
                
        SELECT DISTINCT ON (village)
            distt,
            block_city,
            gp_ward,
            village,
            ru,
            ac_no,
            pc_no
        FROM public.db_table
        WHERE village IS NOT NULL
        AND trim(village) <> ''
        ORDER BY village, updated_at DESC
        ON CONFLICT (village) DO NOTHING
    """)

    conn.commit()

except Exception as e:
    conn.rollback()
    log_debug(f"DB ERROR: {str(e)}")
    print(f"Import failed: {str(e)}")
finally:
    debug_log.close()
    cur.close()
    conn.close()