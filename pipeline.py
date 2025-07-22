import argparse
from ingest import ingest
from transform import transform
import time

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="run data pipeline")
    parser.add_argument('--skip-ingest', action='store_true', help='skip ingestion')
    parser.add_argument('--format', choices=['csv','parquet'], default='csv', help='Output format: csv or parquet (default: csv)')
    args = parser.parse_args()

    print("start pipeline ...")
    if not args.skip_ingest:
        ingest_exec_time = time.perf_counter()
        fetched_pok = ingest()
        ingest_exec_time = time.perf_counter()-ingest_exec_time 
        ingest_status = "success"
    else:
        ingest_status = "failed or skipped"
        ingest_exec_time = 0.0
    
    trans_exec_time = time.perf_counter()
    processed_pok = transform()
    trans_exec_time = time.perf_counter()-trans_exec_time 
    if trans_exec_time > 0:
        trans_status = "success"
    else:
        trans_status = "failed"

    report_lines = [
        "final report",
        f"-ingestion: {ingest_status}",
        f"-execution time of ingestion: {ingest_exec_time}",
        f"-transformation and validation: {trans_status}",
        f"-execution time transformation and validation: {trans_exec_time}",
        f"-number of fetched pokemons: {fetched_pok}",
        f"-numbers of pokemons processed: {processed_pok}"
    ]

    for line in report_lines:
        print(line)
