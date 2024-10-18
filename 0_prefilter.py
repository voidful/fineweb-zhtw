import sys

# from datatrove.executor.slurm import SlurmPipelineExecutor
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

from custom_filter import RegexKeep

#DUMP = "CC-MAIN-2024-26"
#MAIN_OUTPUT_PATH = "/mnt/ccd/CC-MAIN-2024-26/parsed/0_prefilter"

import logging

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
    
custom_url_filter = CustomURLFilter(
    exclusion_writer=JsonlWriter(
        f"{FILTERING_OUTPUT_PATH}/removed/url/{DUMP}",
        output_filename="${rank}.jsonl.gz"
    ),
    url_filterout = set(["&CHANNEL=", ".cn/", "&AID=","=AVSHOW"])
)



if __name__ == '__main__':
    DUMP = sys.argv[1]
    WARC_PATH = sys.argv[2]
    MAIN_OUTPUT_PATH = sys.argv[3]
    
    MAIN_OUTPUT_PATH_WITH_STAGE = os.path.join(MAIN_OUTPUT_PATH,"0_prefilter")
    WARC_PATTERN = os.path.join(WARC_PATH, "./*.warc.gz")
    
    executor = LocalPipelineExecutor(
        pipeline=[
            WarcReader(
                f"/mnt/ccd/CC-MAIN-2024-26/WARC",
                glob_pattern=WARC_PATTERN,
                default_metadata={"dump": DUMP},
            ),
            RegexKeep(r"[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff]{5,}"), # only keep documents with 5 cousecutive chinese chars
            URLFilter(),
            custom_url_filter,
            ParquetWriter(f"{MAIN_OUTPUT_PATH_WITH_STAGE}/output")
        ],
        tasks=64,
        workers=64,
        logging_dir=f"{MAIN_OUTPUT_PATH_WITH_STAGE}/logs/base_processing/{DUMP}",
        
    )
    executor.run()