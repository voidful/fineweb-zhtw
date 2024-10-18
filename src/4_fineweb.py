import os
import re

from datatrove.pipeline.filters import RegexFilter
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.readers import ParquetReader, JsonlReader
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.writers.parquet import ParquetWriter
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.filters.gopher_repetition_filter import find_duplicates
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.typeshelper import Languages
from datatrove.utils.word_tokenizers import load_word_tokenizer

class FineWebQualityFilter(BaseFilter):
    name = "🍷 FineWeb Quality"

    def __init__(
        self,
        exclusion_writer: DiskWriter = None,
        line_punct_thr: float = 0.12,
        line_punct_exclude_zero: bool = False,
        short_line_thr: float = 0.67,
        short_line_length: int = 30,
        char_duplicates_ratio: float = 0.01,
        new_line_ratio: float = 0.3,
        language: str = Languages.english,
    ):
        super().__init__(exclusion_writer)
        self.line_punct_thr = line_punct_thr
        self.line_punct_exclude_zero = line_punct_exclude_zero
        self.short_line_threshold = short_line_thr
        self.short_line_length = short_line_length
        self.char_duplicates_ratio = char_duplicates_ratio
        self.new_line_ratio = new_line_ratio
        self.tokenizer = load_word_tokenizer(language)

    def filter(self, doc) -> bool | tuple[bool, str]:
        stop_chars = (".", "'", '"', "!", "?", 
        "、", "，", "。", "：", "；", "！", "？", "」", ")", ")", "~", "～", "…", "|")

        lines = doc.text.split("\n")
        ratio = sum(1 for line in lines if any(char in line for char in stop_chars)) / len(lines)
        if ratio <= self.line_punct_thr and not (ratio == 0 and self.line_punct_exclude_zero):
            return False, "line_punct_ratio"

        ratio = sum(1 for line in lines if len(line) <= self.short_line_length) / len(lines)
        if ratio >= self.short_line_threshold:
            return False, "short_line_ratio"

        non_empty_lines = [line for line in lines if line.strip() != ""]
        ratio = find_duplicates(non_empty_lines)[1] / len(doc.text.replace("\n", ""))

        if ratio >= self.char_duplicates_ratio:
            return False, "char_dup_ratio"

        words = self.tokenizer.word_tokenize(doc.text)
        new_line = doc.text.count("\n")
        if new_line / len(words) > self.new_line_ratio:
            return False, "list_ratio"

        return True

if __name__ == '__main__':
    DUMP = sys.argv[1]
    MAIN_OUTPUT_PATH = sys.argv[2]

    MAIN_INPUT_PATH_LAST_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "3_c4")
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "4_fineweb")
    FILTERING_OUTPUT_PATH = MAIN_OUTPUT_PATH_WITH_STAGE
    os.makedirs(MAIN_OUTPUT_PATH_WITH_STAGE, exist_ok=True)

    # Initial filtering pipeline - Part 5
    initial_executor_part5 = LocalPipelineExecutor(
        pipeline=[
            JsonlReader(f"{MAIN_INPUT_PATH_LAST_STAGE}/out/", glob_pattern='*.gz'),
            FineWebQualityFilter(
                exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH_WITH_STAGE}/removed/FineWebQuality/{DUMP}"),
                line_punct_thr=0.04,
                line_punct_exclude_zero=False,
                short_line_thr=0.8,
                short_line_length=10,
                char_duplicates_ratio=0.3,
                new_line_ratio=0.3,
                language=Languages.chinese
            ),
            JsonlWriter(f"{MAIN_OUTPUT_PATH_WITH_STAGE}/out")
        ],
        tasks=32,
        workers=32,
        logging_dir=f"{MAIN_OUTPUT_PATH_WITH_STAGE}/logs/base_processing/{DUMP}/part4",
    )

	initial_executor_part5.run()