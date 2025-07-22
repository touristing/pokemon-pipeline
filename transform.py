def transform():
    import pandas as pd
    from datetime import datetime
    import os
    import argparse

    parser = argparse.ArgumentParser(description="Transform and validate pokemon data.")
    parser.add_argument('--format', choices=['csv', 'parquet'], default='csv', help='Output format: csv or parquet (default: csv)')
    args = parser.parse_args()

    # read users to dataframe
    raw_path = "data/raw/pokemon_raw.json"
    df = pd.read_json(raw_path)

    df["pokemon_types"] = df["types"].apply(lambda types: [t["type"]["name"] for t in types])
    # select & rename columns
    df = df[["id", "name", "weight", "height", "pokemon_types"]]
    df.columns = ["pokemon_id", "pokemon_name", "pokemon_weight", "pokemon_height", "pokemon_types"]

    # assert no duplicates in user_id
    if df["pokemon_id"].duplicated().any():
        raise ValueError("Duplicate user_ids found")

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M")
    # save the cleaned data
    os.makedirs("data/processed", exist_ok=True)
    if args.format == 'csv':
        out_path = f"data/processed/pokemons_clean_{timestamp}.csv"
        df.to_csv(out_path, index=False)
    else:
        out_path = f"data/processed/pokemons_clean_{timestamp}.parquet"
        df.to_parquet(out_path, index=False)

    return len(df)

