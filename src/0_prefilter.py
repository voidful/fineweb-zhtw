import sys
import os
import logging

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
    RegexFilter,
)
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.writers.parquet import ParquetWriter
from datatrove.pipeline.writers.disk_base import DiskWriter

from custom_filter import RegexKeep

# Set the logging level to ERROR, so WARNING messages will be suppressed
logging.basicConfig(level=logging.ERROR)

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
    




if __name__ == '__main__':
    DUMP = sys.argv[1]
    #DUMP = "CC-MAIN-2024-26"
    WARC_PATH = sys.argv[2]
    #WARC_PATH = "data/WARC"
    MAIN_OUTPUT_PATH = sys.argv[3]
    #MAIN_OUTPUT_PATH = "data/parsed"
    
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH, "0_prefilter")
    FILTERING_OUTPUT_PATH = MAIN_OUTPUT_PATH_WITH_STAGE
    WARC_PATTERN = "./*.warc.gz"
    os.makedirs(MAIN_OUTPUT_PATH_WITH_STAGE, exist_ok=True)
    
    custom_url_filter = CustomURLFilter(
        exclusion_writer=JsonlWriter(
            f"{FILTERING_OUTPUT_PATH}/removed/url/{DUMP}",
            output_filename="${rank}.jsonl.gz"
        ),
        url_filterout = set(["&CHANNEL=", ".cn/", "&AID=","=AVSHOW"])
    )

    executor = LocalPipelineExecutor(
        pipeline=[
            WarcReader(
                WARC_PATH,
                glob_pattern=WARC_PATTERN,
                default_metadata={"dump": DUMP},
            ),
            RegexKeep(r"[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff]{5,}"), # only keep documents with 5 cousecutive chinese chars
            URLFilter(),
            custom_url_filter,
            ParquetWriter(f"{MAIN_OUTPUT_PATH_WITH_STAGE}/output")
        ],
        tasks=1,
        workers=1,
        start_method='spawn',
        logging_dir=f"{MAIN_OUTPUT_PATH_WITH_STAGE}/logs/base_processing/{DUMP}",
        
    )
    executor.run()