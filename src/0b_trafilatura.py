import os
import sys
import gc

import pandas as pd
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
    RegexFilter,
)
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.writers.parquet import ParquetWriter
from datasets import load_dataset

from custom_filter import RegexKeep

if __name__ == '__main__':
    DUMP = sys.argv[1]
    MAIN_OUTPUT_PATH = sys.argv[2]
    N_CPU = int(sys.argv[3])

    MAIN_INPUT_PATH_LAST_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "0_prefilter")
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "0b_traf")
    os.makedirs(MAIN_OUTPUT_PATH_WITH_STAGE, exist_ok=True)

    dirs = os.listdir(f"{MAIN_INPUT_PATH_LAST_STAGE}/output")

    for out in dirs:
        output_path = f"{MAIN_OUTPUT_PATH_WITH_STAGE}/output/{out}"
        input_path = f"{MAIN_INPUT_PATH_LAST_STAGE}/output/{out}"

        if os.path.exists(output_path):
            print("skipping", out)
            continue

        try:
            ct_dss = load_dataset('parquet', data_files=input_path, keep_in_memory=True)
            print(f"{len(ct_dss)} entries found in {out}")
        except Exception as e:
            print(f"skipping {out} due to error: {e}")
            continue

        gc.collect()

        t = Trafilatura(favour_precision=True)

        def clean_text(example):
            if not example["text"]:
                return example
            try:
                example["text"] = t.extract(example["text"])
            except Exception:
                example["text"] = ""
            return example

        ct_dss = ct_dss.map(clean_text, num_proc=N_CPU)

        def has_text(example):
            return example["text"] is not None and len(example["text"])

        ct_dss = ct_dss.filter(has_text, num_proc=N_CPU)

        print("length", len(ct_dss))
        ct_dss["train"].to_parquet(output_path)
