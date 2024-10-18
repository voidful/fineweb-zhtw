DUMP="CC-MAIN-2024-26"
WARC_PATH="data/WARC/${DUMP}"
MAIN_OUTPUT_PATH="data/parsed"
export PYTHONUTF8=1

python src/0_prefilter.py ${DUMP} ${WARC_PATH} ${MAIN_OUTPUT_PATH}
python src/0b_traf.py ${DUMP} ${MAIN_OUTPUT_PATH}
python src/1_filter_lang.py ${DUMP} ${MAIN_OUTPUT_PATH}
python src/2_gopher.py ${DUMP} ${MAIN_OUTPUT_PATH}
python src/3_c4.py ${DUMP} ${MAIN_OUTPUT_PATH}
python src/4_fineweb.py ${DUMP} ${MAIN_OUTPUT_PATH}
python src/5_tcplus.py ${DUMP} ${MAIN_OUTPUT_PATH}