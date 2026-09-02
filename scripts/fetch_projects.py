"""
Fetches the project list from api.itms21.sk, detects which projects are new
or changed since the last run (using the list endpoint's updatedAt), fetches
full details ONLY for those, and maintains a current-state table in DuckDB
(no daily history — with thousands of projects, a full daily snapshot would
make the repo grow without bound).

Exports the current state to docs/project_data.json for the projects.html page.

Run daily via GitHub Actions (.github/workflows/daily.yml).
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

LIST_URL = "https://api.itms21.sk/public/v1/projekt?limit=-1"
DETAIL_URL_TEMPLATE = "https://api.itms21.sk/public/v1/projekt/id/{id}"

DB_PATH = Path("data/programs.duckdb")  # shared DuckDB file, separate table inside
JSON_OUT_PATH = Path("docs/project_data.json")

TABLE_CURRENT = "projects_current"

MAX_WORKERS = 8           # concurrent detail requests — keep modest to avoid hammering the API
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def fetch_list() -> list[dict]:
    """Call the list endpoint and return all project summary records."""
    resp = requests.get(LIST_URL, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    return payload["results"]


def fetch_detail(project_id: int) -> dict | None:
    """Call the detail endpoint for one project, with basic retry on failure."""
    url = DETAIL_URL_TEMPLATE.format(id=project_id)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == RETRY_ATTEMPTS:
                print(f"  FAILED id={project_id} after {RETRY_ATTEMPTS} attempts: {e}")
                return None
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return None


def extract_eu_sr(financny_plan: list[dict]) -> tuple[float, float]:
    """Split the financial plan into EU vs national (SR) contribution totals."""
    eu = 0.0
    sr = 0.0
    for item in financny_plan or []:
        zdroj_name = (item.get("zdroj") or {}).get("nazovSk", "")
        suma = item.get("suma") or 0
        if "EÚ" in zdroj_name or "EU" in zdroj_name:
            eu += suma
        elif "ŠR" in zdroj_name or "SR" in zdroj_name:
            sr += suma
    return eu, sr


def flatten_project(d: dict) -> dict:
    """Flatten one detailed project record into a single flat row."""
    program = d.get("program") or {}
    prijimatel = d.get("prijimatel") or {}
    eu, sr = extract_eu_sr(d.get("financnyPlan", []))

    return {
        "project_id": d.get("id"),
        "kod": d.get("kod"),
        "nazov": d.get("nazov"),
        "program_skratka": program.get("skratka"),
        "program_nazov": program.get("nazovSk"),
        "prijimatel_nazov": prijimatel.get("nazov"),
        "prijimatel_ico": prijimatel.get("ico"),
        "stav": d.get("stav"),
        "vrealizacii": bool(d.get("vrealizacii")),
        "ukonceny": bool(d.get("ukonceny")),
        "suma_eu": eu,
        "suma_sr": sr,
        "suma_spolu": eu + sr,
        "celkova_zazmluvnena_suma": d.get("celkovaZazmluvnenaSuma"),
        "poskytnute_prostriedky": d.get("poskytnuteProstriedky"),
        "planovany_zaciatok": d.get("planovanaRealizaciaZaciatok"),
        "planovany_koniec": d.get("planovanaRealizaciaKoniec"),
        "created_at": d.get("createdAt"),
        "updated_at": d.get("updatedAt"),
    }


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CURRENT} (
            project_id                  INTEGER PRIMARY KEY,
            kod                         VARCHAR,
            nazov                       VARCHAR,
            program_skratka             VARCHAR,
            program_nazov               VARCHAR,
            prijimatel_nazov            VARCHAR,
            prijimatel_ico              VARCHAR,
            stav                        VARCHAR,
            vrealizacii                 BOOLEAN,
            ukonceny                    BOOLEAN,
            suma_eu                     DOUBLE,
            suma_sr                     DOUBLE,
            suma_spolu                  DOUBLE,
            celkova_zazmluvnena_suma    DOUBLE,
            poskytnute_prostriedky      DOUBLE,
            planovany_zaciatok          BIGINT,
            planovany_koniec            BIGINT,
            created_at                  BIGINT,
            updated_at                  BIGINT
        )
    """)


def get_known_updated_ats(con: duckdb.DuckDBPyConnection) -> dict[int, int]:
    """Map of project_id -> updated_at already stored, to detect changes."""
    rows = con.execute(f"SELECT project_id, updated_at FROM {TABLE_CURRENT}").fetchall()
    return {row[0]: row[1] for row in rows}


def upsert_row(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    con.execute(f"""
        INSERT INTO {TABLE_CURRENT} (
            project_id, kod, nazov, program_skratka, program_nazov,
            prijimatel_nazov, prijimatel_ico, stav, vrealizacii, ukonceny,
            suma_eu, suma_sr, suma_spolu, celkova_zazmluvnena_suma,
            poskytnute_prostriedky, planovany_zaciatok, planovany_koniec,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (project_id) DO UPDATE SET
            kod = excluded.kod,
            nazov = excluded.nazov,
            program_skratka = excluded.program_skratka,
            program_nazov = excluded.program_nazov,
            prijimatel_nazov = excluded.prijimatel_nazov,
            prijimatel_ico = excluded.prijimatel_ico,
            stav = excluded.stav,
            vrealizacii = excluded.vrealizacii,
            ukonceny = excluded.ukonceny,
            suma_eu = excluded.suma_eu,
            suma_sr = excluded.suma_sr,
            suma_spolu = excluded.suma_spolu,
            celkova_zazmluvnena_suma = excluded.celkova_zazmluvnena_suma,
            poskytnute_prostriedky = excluded.poskytnute_prostriedky,
            planovany_zaciatok = excluded.planovany_zaciatok,
            planovany_koniec = excluded.planovany_koniec,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
    """, [
        row["project_id"], row["kod"], row["nazov"], row["program_skratka"], row["program_nazov"],
        row["prijimatel_nazov"], row["prijimatel_ico"], row["stav"], row["vrealizacii"], row["ukonceny"],
        row["suma_eu"], row["suma_sr"], row["suma_spolu"], row["celkova_zazmluvnena_suma"],
        row["poskytnute_prostriedky"], row["planovany_zaciatok"], row["planovany_koniec"],
        row["created_at"], row["updated_at"],
    ])


def sync_projects() -> tuple[int, int, int]:
    """Fetch the list, detect changes, fetch details only for new/changed projects.

    Returns (total_in_list, fetched_count, failed_count).
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    ensure_table(con)

    print("Fetching project list...")
    list_items = fetch_list()
    print(f"List returned {len(list_items)} projects.")

    known = get_known_updated_ats(con)

    to_fetch = []
    for item in list_items:
        pid = item.get("id")
        list_updated_at = item.get("updatedAt")
        if pid not in known or known[pid] != list_updated_at:
            to_fetch.append(pid)

    print(f"{len(to_fetch)} project(s) are new or changed; {len(list_items) - len(to_fetch)} unchanged (skipped).")

    fetched_count = 0
    failed_count = 0

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_detail, pid): pid for pid in to_fetch}
            for i, future in enumerate(as_completed(future_to_id), start=1):
                pid = future_to_id[future]
                detail = future.result()
                if detail is None:
                    failed_count += 1
                    continue
                row = flatten_project(detail)
                upsert_row(con, row)
                fetched_count += 1

                if i % 50 == 0 or i == len(to_fetch):
                    con.commit()  # periodic commit so progress survives an interruption
                    print(f"  Progress: {i}/{len(to_fetch)} processed "
                          f"({fetched_count} ok, {failed_count} failed)")

    con.commit()

    # Remove projects from our table that no longer appear in the list at all (e.g. deleted)
    current_ids = [item.get("id") for item in list_items]
    if current_ids:
        placeholders = ",".join("?" for _ in current_ids)
        con.execute(f"DELETE FROM {TABLE_CURRENT} WHERE project_id NOT IN ({placeholders})", current_ids)

    con.commit()
    con.close()

    return len(list_items), fetched_count, failed_count


def export_to_json() -> None:
    con = duckdb.connect(str(DB_PATH))
    df = con.execute(f"SELECT * FROM {TABLE_CURRENT} ORDER BY suma_spolu DESC").fetchdf()
    con.close()

    JSON_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Convert epoch-millisecond columns to proper ISO date strings for display.
    # DuckDB/pandas keep them as BIGINT internally (fine for storage/queries),
    # but the exported JSON should be human-readable, same as program_data.json.
    date_cols = ["planovany_zaciatok", "planovany_koniec", "created_at", "updated_at"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")

    export = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "projects": json.loads(df.to_json(orient="records", date_format="iso")),
    }

    with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    print(f"Exported {len(df)} projects to {JSON_OUT_PATH}")


def main():
    total, fetched, failed = sync_projects()
    print(f"Done. {total} total in list, {fetched} fetched/updated, {failed} failed.")
    export_to_json()


if __name__ == "__main__":
    main()