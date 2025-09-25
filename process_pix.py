from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

COLUMNS = [
    "NOME ",
    "CPF",
    "COD BANCO",
    "BANCO",
    "AGENCIA",
    "CONTA",
    "TIPO CHAVE PIX",
    "CHAVE PIX",
]

OUTPUT_JSON = Path("pix_output.json")
OUTPUT_XLSX = Path("pix_output.xlsx")


def clean_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return ""
        if value.is_integer():
            return str(int(value))
        return (f"{value:.15g}").strip()
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, str):
        return value.strip()
    if pd.isna(value):
        return ""
    return str(value).strip()


def numeric_or_string(value: str) -> Any:
    if not value:
        return ""
    digits = "".join(ch for ch in value if ch.isdigit())
    if digits and len(digits) == len(value):
        try:
            return int(digits)
        except ValueError:
            return value
    return value


def find_header_row(df: pd.DataFrame) -> Optional[int]:
    for idx, row in df.iterrows():
        for cell in row:
            if isinstance(cell, str) and "CPF" in cell.upper():
                return idx
    return None


def find_name_column(
    df: pd.DataFrame,
    header_idx: int,
    cpf_idx: int,
    header_row: pd.Series,
) -> Optional[int]:
    if cpf_idx <= 0:
        return None
    data = df.iloc[header_idx + 1 :]

    blank_header_cols = [
        col
        for col in range(cpf_idx)
        if (col in header_row.index and pd.isna(header_row[col]))
    ]
    for col in blank_header_cols:
        column_series = data.iloc[:, col]
        if any(isinstance(val, str) and val.strip() for val in column_series):
            return col

    for col in range(cpf_idx):
        column_series = data.iloc[:, col]
        if any(isinstance(val, str) and val.strip() for val in column_series):
            return col
    return None


def extract_rows_from_sheet(df: pd.DataFrame) -> List[Dict[str, Any]]:
    header_idx = find_header_row(df)
    if header_idx is None:
        return []

    header_row = df.iloc[header_idx]
    header_labels: Dict[int, str] = {}
    for col_idx, value in header_row.items():
        if isinstance(value, str):
            header_labels[col_idx] = value.strip().upper()

    cpf_idx = next((idx for idx, label in header_labels.items() if label == "CPF"), None)
    banco_idx = next((idx for idx, label in header_labels.items() if label == "BANCO"), None)
    agencia_idx = next((idx for idx, label in header_labels.items() if label == "AGENCIA"), None)
    conta_idx = next((idx for idx, label in header_labels.items() if label == "CONTA"), None)
    chave_tipo_idx = next(
        (idx for idx, label in header_labels.items() if label == "CHAVE PIX"),
        None,
    )
    chave_val_idx = next((idx for idx, label in header_labels.items() if label == "PIX"), None)

    if cpf_idx is None or banco_idx is None or agencia_idx is None:
        return []

    if conta_idx is None:
        conta_idx = agencia_idx + 1

    name_idx = find_name_column(df, header_idx, cpf_idx, header_row)
    if name_idx is None:
        return []

    records: List[Dict[str, Any]] = []
    for _, row in df.iloc[header_idx + 1 :].iterrows():
        name = clean_value(row.get(name_idx))
        if not name or name.upper().startswith("TOTAL"):
            continue

        cpf = clean_value(row.get(cpf_idx))
        banco_codigo = clean_value(row.get(banco_idx))
        banco_nome = clean_value(row.get(banco_idx + 1)) if banco_idx is not None else ""
        agencia = clean_value(row.get(agencia_idx))
        conta = clean_value(row.get(conta_idx))
        chave_tipo = clean_value(row.get(chave_tipo_idx)) if chave_tipo_idx is not None else ""
        chave_valor = clean_value(row.get(chave_val_idx)) if chave_val_idx is not None else ""

        if not any([cpf, banco_codigo, banco_nome, agencia, conta, chave_tipo, chave_valor]):
            continue

        record = {
            "NOME ": name,
            "CPF": cpf,
            "COD BANCO": numeric_or_string(banco_codigo),
            "BANCO": banco_nome,
            "AGENCIA": agencia,
            "CONTA": conta,
            "TIPO CHAVE PIX": chave_tipo,
            "CHAVE PIX": numeric_or_string(chave_valor) if chave_valor.isdigit() else chave_valor,
        }
        records.append(record)

    return records


def process_workbook(path: Path) -> List[Dict[str, Any]]:
    try:
        xls = pd.ExcelFile(path)
    except Exception as exc:  # pragma: no cover - logging / debugging purpose
        print(f"Não foi possível abrir {path.name}: {exc}")
        return []

    records: List[Dict[str, Any]] = []
    for sheet_name in xls.sheet_names:
        df = xls.parse(sheet_name=sheet_name, header=None)
        records.extend(extract_rows_from_sheet(df))
    return records


def main() -> None:
    files = sorted(
        path
        for path in Path('.').glob('*.xlsx')
        if path.name != OUTPUT_XLSX.name
    )

    all_records: List[Dict[str, Any]] = []
    for file_path in files:
        all_records.extend(process_workbook(file_path))

    df = pd.DataFrame(all_records, columns=COLUMNS)
    df.fillna("", inplace=True)

    OUTPUT_JSON.write_text(json.dumps(all_records, ensure_ascii=False, indent=2))
    df.to_excel(OUTPUT_XLSX, index=False)


if __name__ == "__main__":
    main()
