import os
import pandas as pd
import pathlib
import json


def import_single_json_to_parquet(src_path, dest_path):
    with open(src_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Support both list-of-dict and dict-of-list JSON
    if isinstance(data, dict):
        df = pd.DataFrame.from_dict(data)
    else:
        df = pd.DataFrame(data)
    df.to_parquet(dest_path, index=False)
    print(f"Imported {src_path} -> {dest_path}")


def import_json_to_parquet(source_dir, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    for file in os.listdir(source_dir):
        if file.endswith(".json"):
            src_path = os.path.join(source_dir, file)
            dest_path = os.path.join(target_dir, pathlib.Path(file).with_suffix(".parquet"))
            import_single_json_to_parquet(src_path, dest_path)
