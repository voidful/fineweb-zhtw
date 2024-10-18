from datatrove.pipeline.filters import RegexFilter
import re
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.readers import ParquetReader, JsonlReader
from datatrove.executor.local import LocalPipelineExecutor
import os
from datatrove.pipeline.writers.parquet import ParquetWriter

DUMP = "CC-MAIN-2024-33"
MAIN_INPUT_PATH = f"/mnt/ccd/{DUMP}/parsed/1_filter_sc2"
MAIN_OUTPUT_PATH = f"/mnt/ccd/{DUMP}/parsed/2_gopher"
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

chinese_stop_words = [
 "的", "了", "和", "是", "就", "都", "而", "及", "與", "這", "其", "但", "並", "個", "我",
 "你", "他", "她", "它", "們", "我們", "你們", "他們", "她們", "它們", "在", "有", "人",
 "這個", "那個", "如果", "因為", "所以", "可以", "沒有", "很", "非常", "得", "著", "過", "為", "再",
 "吧", "呢", "啊", "哪", "那", "麼", "什麼", "誰", "哪裡", "哪裡", "怎麼", "怎麼樣", "為什麼", "將"
]


# Initial filtering pipeline - Part 2
initial_executor_part2 = LocalPipelineExecutor(
    pipeline=[
        JsonlReader(f"{MAIN_INPUT_PATH}/out/", glob_pattern='*.gz'),
        # GopherQualityFilter(
            
        #     exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/2_gopher_min_max_len/{DUMP}"),
        #     language="zh",
        # ),
        # GopherQualityFilter(
        #     min_avg_word_length=1,  # doesn't filter out anything any way
        #     max_avg_word_length=4, # removed because we have some bilingual content
        # )
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
        #47423753
        #450562782
        # GopherQualityFilter(
        #     max_bullet_lines_ratio=0.9,
        #     exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/2_gopher_bullet_ratio/{DUMP}"),
        #     language="zh",
        # ),
        # GopherQualityFilter(
            
        # ),
        # GopherQualityFilter(
        #      # Remove this for Chinese
        #     language="zh",
        # ),
        # GopherQualityFilter(
            
        #     exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/2_gopher_stop_words/{DUMP}"),
        #     language="zh",
        # ),
        
        JsonlWriter(f"{MAIN_OUTPUT_PATH}/out")
    ],
    tasks=32,
    workers=32,
    logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing/{DUMP}/",
)
if __name__ == '__main__':
    initial_executor_part2.run()

