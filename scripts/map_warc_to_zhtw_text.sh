DUMP = "CC-MAIN-2024-26"
WARC_PATH = "data/WARC"
MAIN_OUTPUT_PATH = "data/parsed"

python3 0_prefilter.py ${DUMP} ${WARC_PATH} ${MAIN_OUTPUT_PATH}
python3 0b_traf.py ${DUMP} ${MAIN_OUTPUT_PATH}
python3 1_filter_lang.py ${DUMP} ${MAIN_OUTPUT_PATH}
python3 2_gopher.py ${DUMP} ${MAIN_OUTPUT_PATH}
python3 3_c4_final.py ${DUMP} ${MAIN_OUTPUT_PATH}
python3 4_fineweeb_final.py ${DUMP} ${MAIN_OUTPUT_PATH}
python3 5_tcplus.py ${DUMP} ${MAIN_OUTPUT_PATH}


