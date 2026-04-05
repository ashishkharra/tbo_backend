import psycopg2
import csv
import sys
from io import StringIO
import json

if len(sys.argv) < 4:
    print("Usage: python import_erolldata.py <csv> <ac_no> <data_id>")
    sys.exit(1)


def txt_or_o(v):
    if v is None:
        return "O"
    v = str(v).strip()
    return v if v else "O"


def safe_int(v, default=0):
    if v is None:
        return default
    try:
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except (ValueError, TypeError):
        return default


def normalize(v):
    if v is None:
        return ""
    return str(v).strip().lower()


def normalize_hno(v):
    if v is None:
        return "0"

    s = str(v).strip().lower()
    s = s.replace(" ", "")

    if s == "":
        return "0"

    # 1.0 -> 1
    try:
        if "." in s:
            f = float(s)
            if f.is_integer():
                s = str(int(f))
    except Exception:
        pass

    return s if s else "0"


csv_file = sys.argv[1]
AC_NO = int(sys.argv[2])
DATA_ID = int(sys.argv[3])

conn = psycopg2.connect(
    dbname="tbo_data",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

buffer = StringIO()
writer = csv.writer(buffer, lineterminator="\n")

columns = [
    "data_id",
    "card_id",
    "ac_no",
    "bhag_no",
    "sec_no",
    "section",
    "hof",
    "epic",
    "vsno",
    "vname",
    "sex",
    "age",
    "dob",
    "relation",
    "rname",
    "hno",
    "phone1",
    "phone2",
    "cast_cat",
    "castId",
    "castId_surname",
    "subCast",
    "oldNew",
    "familyId",
    "hof_relation",
    "aadhar_no",
    "surname",
    "proff_id",
    "proff_city",
    "bank_name",
    "acc_no",
    "ifsc",
    "upi_id",
    "star",
    "photo",
    "pdob_verify",
    "msg",
    "voter_view",
    "v_parchi",
    "worker_id",
    "edu_id",
    "mapping"
]

writer.writerow(columns)

with open(csv_file, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    reader.fieldnames = [h.strip().lower().replace(" ", "_") for h in reader.fieldnames]

    # family key -> sequence id
    family_state = {}
    next_family_id = 1

    for r in reader:
        if not any(r.values()):
            continue

        mapping_obj = {}

        # normalize key parts
        ac_no = normalize(r.get("ac_no")) or str(AC_NO)
        bhag = normalize(r.get("bhag"))
        sec_no = normalize(r.get("section_no") or r.get("sec_no"))
        house_part = normalize_hno(r.get("hno"))

        # family key
        family_key = (ac_no, bhag, sec_no, house_part)

        if family_key not in family_state:
            family_state[family_key] = next_family_id
            next_family_id += 1

        family_id = family_state[family_key]

        # parse vsno safely
        vsno_val = 0
        for key_name in ["vsno", "v_sno", "voter_sno", "serial_no"]:
            if key_name in r and r[key_name] not in (None, ""):
                vsno_val = safe_int(r[key_name], 0)
                break

        # surname logic
        SURNAME = None
        name_v = (r.get("vname_hi") or r.get("vname") or "").strip()
        name_r = (r.get("rname") or "").strip()

        v_parts = name_v.split()
        r_parts = name_r.split()

        v_surname = None
        r_surname = None

        if len(v_parts) > 1 and len(r_parts) > 1:
            v_surname = v_parts[-1]
            r_surname = r_parts[-1]
        elif len(v_parts) <= 1 and len(r_parts) > 1:
            v_surname = r_parts[-1]
            r_surname = r_parts[-1]
        elif len(v_parts) > 1 and len(r_parts) <= 1:
            v_surname = v_parts[-1]
            r_surname = v_parts[-1]

        if v_surname or r_surname:
            SURNAME = json.dumps({"v": v_surname, "r": r_surname}, ensure_ascii=False)

        writer.writerow([
            r.get("data_id") if r.get("data_id") else DATA_ID,
            r.get("card_id"),
            safe_int(ac_no, AC_NO),
            safe_int(bhag, 0),
            safe_int(sec_no, 0),
            r.get("section"),
            safe_int(r.get("hof")),
            r.get("epic"),
            vsno_val,
            r.get("vname_hi") or r.get("vname"),
            r.get("sex"),
            safe_int(r.get("age")),
            r.get("dob"),
            r.get("relation"),
            r.get("rname"),
            house_part,
            r.get("mobile_1"),
            r.get("mobile_2"),
            txt_or_o(r.get("cast_cat")),
            txt_or_o(r.get("castid")),
            txt_or_o(r.get("castid_surname") or r.get("castid__surname")),
            r.get("subcast"),
            safe_int(r.get("old/new") or r.get("oldnew")),
            family_id,
            r.get("hof_relation"),
            r.get("aadhar_card_no"),
            SURNAME,
            txt_or_o(r.get("proff_id")),
            r.get("proff_city"),
            r.get("bank_name"),
            r.get("acc_no"),
            r.get("ifsc"),
            r.get("upi_id"),
            safe_int(r.get("star")),
            r.get("photo"),
            safe_int(r.get("pdob_verify")),
            r.get("msg"),
            r.get("voter_view"),
            r.get("v_parchi"),
            safe_int(r.get("worker_id")),
            txt_or_o(r.get("edu_id")),
            json.dumps(mapping_obj, ensure_ascii=False)
        ])

cur.execute("""
    CREATE TEMP TABLE temp_eroll (LIKE eroll_db INCLUDING DEFAULTS)
    ON COMMIT DROP;
""")

buffer.seek(0)

cur.copy_expert(f"""
    COPY temp_eroll ({", ".join(columns)})
    FROM STDIN WITH CSV HEADER
""", buffer)

cur.execute(f"""
    WITH upserted AS (
        INSERT INTO eroll_db ({", ".join(columns)})
        SELECT DISTINCT ON (data_id, ac_no, bhag_no, sec_no, epic)
               {", ".join(columns)}
        FROM temp_eroll
        ORDER BY data_id, ac_no, bhag_no, sec_no, epic
        ON CONFLICT (data_id, ac_no, bhag_no, sec_no, epic)
        DO UPDATE SET
            card_id = EXCLUDED.card_id,
            section = EXCLUDED.section,
            epic = EXCLUDED.epic,
            vsno = EXCLUDED.vsno,
            vname = EXCLUDED.vname,
            sex = EXCLUDED.sex,
            age = EXCLUDED.age,
            dob = EXCLUDED.dob,
            relation = EXCLUDED.relation,
            rname = EXCLUDED.rname,
            hno = EXCLUDED.hno,
            phone1 = EXCLUDED.phone1,
            phone2 = EXCLUDED.phone2,
            castId = EXCLUDED.castId,
            cast_cat = EXCLUDED.cast_cat,
            castId_surname = EXCLUDED.castId_surname,
            subCast = EXCLUDED.subCast,
            oldNew = EXCLUDED.oldNew,
            familyId = EXCLUDED.familyId,
            hof_relation = EXCLUDED.hof_relation,
            aadhar_no = EXCLUDED.aadhar_no,
            surname = EXCLUDED.surname,
            star = EXCLUDED.star,
            photo = EXCLUDED.photo,
            pdob_verify = EXCLUDED.pdob_verify,
            msg = EXCLUDED.msg,
            voter_view = EXCLUDED.voter_view,
            v_parchi = EXCLUDED.v_parchi,
            worker_id = EXCLUDED.worker_id,
            edu_id = EXCLUDED.edu_id,
            proff_id = EXCLUDED.proff_id,
            proff_city = EXCLUDED.proff_city,
            bank_name = EXCLUDED.bank_name,
            acc_no = EXCLUDED.acc_no,
            ifsc = EXCLUDED.ifsc,
            upi_id = EXCLUDED.upi_id,
            mapping = EXCLUDED.mapping,
            update_by = CURRENT_TIMESTAMP
        RETURNING id
    )
    SELECT
        COALESCE(MIN(id), 0) AS min_id,
        COALESCE(MAX(id), 0) AS max_id,
        COUNT(*) AS affected_count
    FROM upserted;
""")

result = cur.fetchone()
min_id, max_id, affected_count = result

# Re-generate clean familyId sequence from DB
# same (ac_no, bhag_no, sec_no, hno) => same familyId
# output always 1,2,3...
cur.execute("""
WITH family_groups AS (
    SELECT
        id,
        DENSE_RANK() OVER (
            ORDER BY
                ac_no,
                bhag_no,
                sec_no,
                COALESCE(NULLIF(TRIM(hno::text), ''), '0')
        ) AS new_family_id
    FROM eroll_db
    WHERE data_id = %s
)
UPDATE eroll_db e
SET familyId = fg.new_family_id
FROM family_groups fg
WHERE e.id = fg.id;
""", (DATA_ID,))

# HOF update:
# family me age < 70 wala eldest first
# agar koi < 70 nahi hai to overall eldest
cur.execute("""
WITH ranked AS (
    SELECT
        id,
        data_id,
        familyId,
        age,
        ROW_NUMBER() OVER (
            PARTITION BY data_id, familyId
            ORDER BY
                CASE
                    WHEN age IS NOT NULL AND age < 70 THEN 0
                    ELSE 1
                END ASC,
                age DESC NULLS LAST,
                id ASC
        ) AS rn
    FROM eroll_db
    WHERE data_id = %s
      AND familyId IS NOT NULL
)
UPDATE eroll_db e
SET hof = CASE WHEN r.rn = 1 THEN 1 ELSE 0 END
FROM ranked r
WHERE e.id = r.id
  AND e.data_id = r.data_id;
""", (DATA_ID,))

cur.execute("""
UPDATE eroll_db
SET hof = 0
WHERE data_id = %s
  AND hof IS NULL;
""", (DATA_ID,))

cur.execute("SELECT create_eroll_mapping_partition();")

cur.execute("""
INSERT INTO eroll_mapping (
    data_id, ac_id, bhag_no, sec_no, section, ru
)
SELECT
    e.data_id,
    e.ac_no,
    e.bhag_no,
    e.sec_no,
    e.section,
    0
FROM eroll_db e
WHERE e.data_id = %s
AND NOT EXISTS (
    SELECT 1
    FROM eroll_mapping m
    WHERE m.data_id = e.data_id
      AND m.ac_id = e.ac_no
      AND m.bhag_no = e.bhag_no
      AND m.sec_no = e.sec_no
      AND m.section = e.section
)
GROUP BY e.data_id, e.ac_no, e.bhag_no, e.sec_no, e.section;
""", (DATA_ID,))

conn.commit()

if affected_count > 0:
    new_range = {"from": min_id, "to": max_id}

    cur.execute("""
        SELECT data_range
        FROM dataid_importmaster
        WHERE data_id = %s
        FOR UPDATE
    """, (DATA_ID,))
    row = cur.fetchone()
    print("Existing row:", row)

    existing_range = []
    if row and row[0] is not None:
        val = row[0]
        if isinstance(val, str):
            try:
                existing_range = json.loads(val)
            except Exception:
                existing_range = []
        elif isinstance(val, list):
            existing_range = val
        else:
            existing_range = []

    if not isinstance(existing_range, list):
        existing_range = []

    existing_range.append(new_range)
    existing_range.sort(key=lambda x: x["from"])

    merged = []
    for item in existing_range:
        if not merged:
            merged.append(item)
        else:
            last = merged[-1]
            if item["from"] <= last["to"] + 1:
                last["to"] = max(last["to"], item["to"])
            else:
                merged.append(item)

    cur.execute("""
        INSERT INTO dataid_importmaster (data_id, data_range, updated_at)
        VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
        ON CONFLICT (data_id)
        DO UPDATE SET
            data_range = EXCLUDED.data_range,
            updated_at = CURRENT_TIMESTAMP
    """, (DATA_ID, json.dumps(merged)))

    print("Rows affected:", cur.rowcount)
    conn.commit()

cur.close()
conn.close()

print("Import complete with clean integer familyId sequence and HOF updated successfully.")