"""
Fetches the current program list from api.itms21.sk and exports it to JSON
for the published web page. Runs daily via GitHub Actions.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://api.itms21.sk/public/v1/program?limit=-1"
JSON_OUT_PATH = Path("docs/program_data.json")


def fetch_programs() -> list[dict]:
    """Call the API and return the list of program records."""
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload["results"]


def flatten(records: list[dict]) -> pd.DataFrame:
    """Flatten the nested JSON into a tidy DataFrame with one row per program."""
    df = pd.json_normalize(records, sep="_")

    cols = {
        "id": "program_id",
        "kod": "kod",
        "nazovSk": "nazov_sk",
        "nazovEn": "nazov_en",
        "skratka": "skratka",
        "sumaEu": "suma_eu",
        "sumaSr": "suma_sr",
        "sumaSpolu": "suma_spolu",
        "kodCCI": "kod_cci",
        "typProgramu_typ": "typ_programu",
        "riadiaciOrgan_nazov": "riadiaci_organ",
        "riadiaciOrgan_subjekt_nazov": "subjekt_nazov",
        "riadiaciOrgan_subjekt_ico": "ico",
        "riadiaciOrgan_subjekt_adresa_obec": "obec",
        "createdAt": "created_at",
        "updatedAt": "updated_at",
    }
    df_clean = df.reindex(columns=list(cols.keys())).rename(columns=cols)

    for c in ["suma_eu", "suma_sr", "suma_spolu"]:
        df_clean[c] = pd.to_numeric(df_clean[c])

    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], unit="ms")
    df_clean["updated_at"] = pd.to_datetime(df_clean["updated_at"], unit="ms")

    return df_clean.sort_values("suma_spolu", ascending=False).reset_index(drop=True)


def export_to_json(df: pd.DataFrame) -> None:
    """Write the current snapshot to JSON for the web page to fetch."""
    JSON_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    export = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "programs": json.loads(df.to_json(orient="records", date_format="iso")),
    }

    with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)

    print(f"Exported {len(df)} programs to {JSON_OUT_PATH}")


def main():
    print("Fetching programs...")
    records = fetch_programs()
    df = flatten(records)
    export_to_json(df)


if __name__ == "__main__":
    main()