import pandas as pd

def normalize_df(df, expected_cols):
    # 1. Remove pandas suffixes like _x, _y, or _m123
    df.columns = [c.split("_m")[0] for c in df.columns]
    df.columns = [c.split(".")[0] for c in df.columns]
    df.columns = [c.replace("\t", "") for c in df.columns]  # bad TSV headers

    # 2. Rename ANY "Unnamed:*" to index_raw
    for col in list(df.columns):
        if col.lower().startswith("unnamed"):
            df = df.rename(columns={col: "index_raw"})

    # 3. If index_raw still missing, create it
    if "index_raw" not in df.columns:
        df.insert(0, "index_raw", range(len(df)))

    # 4. Keep only expected schema columns
    df = df[[c for c in df.columns if c in expected_cols]]

    return df


