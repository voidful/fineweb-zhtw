from datatrove.pipeline.filters import RegexFilter
import re
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.readers import JsonlReader, ParquetReader
from datatrove.executor.local import LocalPipelineExecutor
import os
from datatrove.pipeline.writers.parquet import ParquetWriter
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter

from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
    RegexFilter,
    C4QualityFilter,
)
from datatrove.pipeline.writers.jsonl import JsonlWriter

DUMP = "CC-MAIN-2024-33"
MAIN_INPUT_PATH = f"/mnt/ccd/{DUMP}/parsed/4_fineweb"
MAIN_OUTPUT_PATH = f"/mnt/ccd/{DUMP}/parsed/5_tcplus"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}"

#f
# Create a regex pattern from the simplified_only set
filterout_pattern = b'\xe4\xba\x91(?![:\xef\xbc\x9a])|\xe6\xb0\x94|\xe6\x84\xbf|\xe4\xbd\x8d\xe4\xba\x8e|\xe8\x87\xb4\xe5\x8a\x9b\xe4\xba\x8e|\xe5\xb1\xac\xe4\xba\x8e|\xe5\x94\xae\xe5\x90\x8e|\xe8\x8c\x83\xe5\x9c\x8d|\xe5\x88\xb6\xe4\xbd\x9c|\xe7\x94\xb1\xe4\xba\x8e|\xe5\xb0\x8d\xe4\xba\x8e|\xe7\x94\xa8\xe4\xba\x8e|\xe5\x8d\x9a\xe5\xae\xa2|\xe6\x98\x93\xe4\xba\x8e|\xe5\x82\xa2\xe4\xbf\xac|\xe4\xbf\xa1\xe6\x81\xaf|\xe9\x97\x9c\xe4\xba\x8e|\xe5\xb9\xb6|\xe9\x87\x87\xe6\xa8\xa3|\xe5\x89\x8d\xe5\x90\x8e|\xe8\xa6\x96\xe9\xa0\xbb|\xe4\xbb\xb2\xe6\x9c\x89|\xe4\xba\xba\xe6\xb0\x91\xe6\x97\xa5\xe5\xa0\xb1|\xe4\xb9\x88|\xe8\x81\xaf\xe7\xb3\xbb|@163.com|\xe9\x87\x87\xe8\xa8\xaa|\xe9\x87\x87\xe7\x94\xa8|\xe4\xbb\xa5\xe5\x90\x8e|\xe5\xbb\xba\xe4\xba\x8e|\xe7\xad\x91|\xe7\xb2\x98|\xe5\xa2\xbb|\xe5\x85\xac\xe7\x9c\xbe\xe8\x99\x9f'.decode('utf-8')
filterout_plus18_pattern=b'\xe5\x8c\x85\xe9\xa4\x8a|\xe8\xa3\xb8\xe8\x81\x8a|\xe6\xb7\xab\xe7\x8b\xbc|\xe5\x90\x8c\xe5\x9f\x8e|\xe6\xb7\xab\xe5\xa5\xb3|\xe8\x95\xa9\xe5\xa9\xa6|\xe8\x82\x9b\xe4\xba\xa4|\xe5\x8f\xa3\xe7\x88\x86'.decode("utf8")

#url &CHANNEL= .cn

class CustomURLFilter(BaseFilter):
    name = "🍷 FineWeb Quality"

    def __init__(
        self,
        exclusion_writer: DiskWriter = None,
        url_filterout: set = set()
    ):
        super().__init__(exclusion_writer)
        self.url_filterout = url_filterout

    def filter(self, doc) -> bool | tuple[bool, str]:
        
        url = doc.metadata.get("url")
        
        if any(u in url for u in self.url_filterout):
            return False, "url_filterout"

        return True
    
# Create the RegexFilter to remove documents with Simplified Chinese
simplified_filter = RegexFilter(
    regex_exp=filterout_pattern,
    exclusion_writer=JsonlWriter(
        f"{FILTERING_OUTPUT_PATH}/removed/neg1_tcfine/{DUMP}",
        output_filename="${rank}.jsonl.gz"
    )
)
plus18_filter = RegexFilter(
    regex_exp=filterout_plus18_pattern,
    exclusion_writer=JsonlWriter(
        f"{FILTERING_OUTPUT_PATH}/removed/neg1_plus18/{DUMP}",
        output_filename="${rank}.jsonl.gz"
    )
)

custom_url_filter = CustomURLFilter(
    exclusion_writer=JsonlWriter(
        f"{FILTERING_OUTPUT_PATH}/removed/url/{DUMP}",
        output_filename="${rank}.jsonl.gz"
    ),
    url_filterout = set(["&CHANNEL=", ".cn/", "&AID=","=AVSHOW"])
)
if __name__ == '__main__':
    DUMP = sys.argv[1]
    MAIN_OUTPUT_PATH = sys.argv[2]

    MAIN_INPUT_PATH_LAST_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "3_c4")
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH, DUMP, "4_fineweb")
    FILTERING_OUTPUT_PATH = MAIN_OUTPUT_PATH_WITH_STAGE
    os.makedirs(MAIN_OUTPUT_PATH_WITH_STAGE, exist_ok=True)

    initial_executor_part1 = LocalPipelineExecutor(
        pipeline=[
            JsonlReader(f"{MAIN_INPUT_PATH}/out/", glob_pattern='*.gz'),
            simplified_filter,
            plus18_filter,
            custom_url_filter,
            JsonlWriter(f"{MAIN_OUTPUT_PATH}/out"),
            
        ],
        tasks=32,
        workers=32,
        logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing//{DUMP}/part1",
    )

	initial_executor_part1.run()
    
    
