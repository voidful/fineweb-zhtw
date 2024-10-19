import os
import re
import sys

from datatrove.pipeline.filters import RegexFilter
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.readers import ParquetReader, JsonlReader
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.writers.parquet import ParquetWriter
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

if __name__ == '__main__':
    DUMP = sys.argv[1]
    MAIN_OUTPUT_PATH = sys.argv[2]
    N_CPU = int(sys.argv[3])

    MAIN_INPUT_PATH_LAST_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "2_gopher")
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "3_c4")
    FILTERING_OUTPUT_PATH = MAIN_OUTPUT_PATH_WITH_STAGE
    os.makedirs(MAIN_OUTPUT_PATH_WITH_STAGE, exist_ok=True)
    # Initial filtering pipeline - Part 3
    initial_executor_part3 = LocalPipelineExecutor(
        pipeline=[
            JsonlReader(f"{MAIN_INPUT_PATH_LAST_STAGE}/output/", glob_pattern='*.gz'),
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
            JsonlWriter(f"{MAIN_OUTPUT_PATH_WITH_STAGE}/output")
        ],
        tasks=N_CPU,
        workers=N_CPU,
        logging_dir=f"{MAIN_OUTPUT_PATH_WITH_STAGE}/logs/base_processing/{DUMP}/part3",
    )
    initial_executor_part3.run()

