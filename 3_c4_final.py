from datatrove.pipeline.filters import RegexFilter
import re
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.readers import ParquetReader, JsonlReader
from datatrove.executor.local import LocalPipelineExecutor
import os
from datatrove.pipeline.writers.parquet import ParquetWriter

DUMP = "CC-MAIN-2024-33"
MAIN_INPUT_PATH = f"/mnt/ccd/{DUMP}/parsed/2_gopher"
MAIN_OUTPUT_PATH = f"/mnt/ccd/{DUMP}/parsed/3_c4"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}"


from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
    RegexFilter,
    C4QualityFilter,
)
from datatrove.pipeline.dedup import MinhashDedupCluster, MinhashDedupFilter, MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.formatters import PIIFormatter

from datatrove.utils.typeshelper import Languages

# Initial filtering pipeline - Part 3
initial_executor_part3 = LocalPipelineExecutor(
    pipeline=[
        JsonlReader(f"{MAIN_INPUT_PATH}/out/", glob_pattern='*.gz'),
        C4QualityFilter(
            split_paragraph=True,  
            remove_citations=False, 
            filter_no_terminal_punct=False,
            min_num_sentences=-1,  
            min_words_per_line=-1, 
            max_word_length=-1,
            filter_lorem_ipsum=False,  # Probably not relevant for Chinese
            filter_javascript=True,  
            filter_curly_bracket=True,  
            filter_policy=True,
            bracket_ratio=0.01,
            language="zh",
            exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/3_c4/{DUMP}"),
        ),
        JsonlWriter(f"{MAIN_OUTPUT_PATH}/out")
    ],
    tasks=32,
    workers=32,
    logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing/{DUMP}/part3",
)
if __name__ == '__main__':
    initial_executor_part3.run()

