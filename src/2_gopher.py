import re
import os

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

chinese_stop_words = [
 "的", "了", "和", "是", "就", "都", "而", "及", "與", "這", "其", "但", "並", "個", "我",
 "你", "他", "她", "它", "們", "我們", "你們", "他們", "她們", "它們", "在", "有", "人",
 "這個", "那個", "如果", "因為", "所以", "可以", "沒有", "很", "非常", "得", "著", "過", "為", "再",
 "吧", "呢", "啊", "哪", "那", "麼", "什麼", "誰", "哪裡", "哪裡", "怎麼", "怎麼樣", "為什麼", "將"
]

if __name__ == '__main__':
    DUMP = sys.argv[1]
    MAIN_OUTPUT_PATH = sys.argv[2]

    MAIN_INPUT_PATH_LAST_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "1_filter_lang")
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "2_gopher")
    FILTERING_OUTPUT_PATH = MAIN_OUTPUT_PATH_WITH_STAGE
    os.makedirs(MAIN_OUTPUT_PATH_WITH_STAGE, exist_ok=True)
    
    # Initial filtering pipeline - Part 2
    initial_executor_part2 = LocalPipelineExecutor(
        pipeline=[
            JsonlReader(f"{MAIN_INPUT_PATH_LAST_STAGE}/output/", glob_pattern='*.gz'),
            GopherQualityFilter(
                min_doc_words=50,
                max_doc_words=100000,
                max_symbol_word_ratio=0.1,
                exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/2_gopher/{DUMP}"),
                language="zh",
                max_ellipsis_lines_ratio=0.3,
                ellipsis_line_word_threshold=100, # if BELOW this threshold and end with ellipsis, count as a bad line, else count as a good line.
                max_non_alpha_words_ratio=0, 
                min_stop_words=1,
                stop_words=chinese_stop_words,
            ),           
            JsonlWriter(f"{MAIN_OUTPUT_PATH_WITH_STAGE}/output")
        ],
        tasks=32,
        workers=32,
        logging_dir=f"{MAIN_OUTPUT_PATH_WITH_STAGE}/logs/base_processing/{DUMP}/",
    )

    initial_executor_part2.run()

