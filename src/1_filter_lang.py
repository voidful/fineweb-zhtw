import os
import sys
import re
import gc
from functools import partial

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.readers import ParquetReader
from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
    RegexFilter,
    C4QualityFilter,
)
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.writers.parquet import ParquetWriter
from datatrove.pipeline.dedup import (
    MinhashDedupCluster, 
    MinhashDedupFilter, 
    MinhashDedupSignature
)
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.formatters import PIIFormatter
from datatrove.utils.typeshelper import Languages

from regex_keep import RegexKeep

def read_ts_character_mappings(file_path):
    traditional, simplified = set(), set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            chars = line.strip().split('\t')
            if len(chars) >= 2:
                trad, simps = chars[0], chars[1].split()
                traditional.add(trad)
                simplified.update(simps)
    return traditional, simplified

def read_st_character_mappings(file_path):
    traditional, simplified = set(), set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            chars = line.strip().split('\t')
            if len(chars) >= 2:
                simp, trads = chars[0], chars[1].split()
                simplified.add(simp)
                traditional.update(trads)
    return traditional, simplified

def read_traditional_characters(file_path):
    additional_set = set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            additional_set.update(line.strip())
    return additional_set

if __name__ == '__main__':
    
    DUMP = sys.argv[1]
    MAIN_OUTPUT_PATH = sys = argv[2]
    
    MAIN_INPUT_PATH_LAST_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "0b_traf")
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "1_filter_lang")
    FILTERING_OUTPUT_PATH = MAIN_OUTPUT_PATH_WITH_STAGE

    # Read the character mappings from both files
    ts_traditional, ts_simplified = read_ts_character_mappings(
        os.path.join('src', "TSCharacters.txt")
    )
    st_traditional, st_simplified = read_st_character_mappings(
        os.path.join('src', "STCharacters.txt")
    )
    # Combine the sets from both files
    traditional = ts_traditional.union(st_traditional)
    simplified = ts_simplified.union(st_simplified)

    # Read the additional traditional characters
    additional_file = os.path.join(SRC_PATH, "trad.txt")
    additional_set = read_traditional_characters(additional_file)

    # Calculate the characters for the filters
    white_list = "床峰群秘霉庄痴雇简体踪"
    white_list_set = set(white_list)
    simplified_only = simplified - traditional - white_list_set - additional_set

    # Create regex patterns for filtering
    simplified_pattern = '|'.join(re.escape(char) for char in simplified_only)
    traditional_pattern = '|'.join(re.escape(char) for char in traditional)

    # Define filters
    simplified_chinese_filter = RegexFilter(
        regex_exp=simplified_pattern,
        exclusion_writer=JsonlWriter(
            f"{FILTERING_OUTPUT_PATH}/removed/neg1_simplified/{DUMP}",
            output_filename="${rank}.jsonl.gz"
        )
    )

    traditional_chinese_filter = RegexKeep(
        regex_exp=traditional_pattern,
        exclusion_writer=JsonlWriter(
            f"{FILTERING_OUTPUT_PATH}/removed/neg1_notraditional/{DUMP}",
            output_filename="${rank}.jsonl.gz"
        )
    )

    # Run the pipeline
    initial_executor_part1 = LocalPipelineExecutor(
        pipeline=[
            ParquetReader(f"{MAIN_INPUT_PATH_LAST_STAGE}/output", glob_pattern='*.parquet'),
            LanguageFilter(
                languages=["zh"], 
                language_threshold=0, 
                exclusion_writer=JsonlWriter(
                    f"{FILTERING_OUTPUT_PATH}/removed/neg1_notlang/{DUMP}",
                    output_filename="${language}/${rank}.jsonl.gz"
                )
            ),
            simplified_chinese_filter,
            traditional_chinese_filter,
            JsonlWriter(f"{MAIN_OUTPUT_PATH_WITH_STAGE}/output"),
        ],
        tasks=32,
        workers=32,
        logging_dir=f"{MAIN_OUTPUT_PATH_WITH_STAGE}/logs/base_processing/{DUMP}/part1",
    )
    initial_executor_part1.run()
