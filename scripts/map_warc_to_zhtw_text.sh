DUMP="FINEWINE2"
WARC_PATH="data/WARC/${DUMP}"
MAIN_OUTPUT_PATH="data/parsed"
N_CPU=15
export PYTHONUTF8=1

python3 src/1_finewine2.py ${DUMP} ${MAIN_OUTPUT_PATH} ${N_CPU}
python3 src/2_gopher.py ${DUMP} ${MAIN_OUTPUT_PATH} ${N_CPU}
python3 src/3_c4.py ${DUMP} ${MAIN_OUTPUT_PATH} ${N_CPU}
python3 src/4_fineweb.py ${DUMP} ${MAIN_OUTPUT_PATH} ${N_CPU}
# For simple aggregation of the files, we set CPU=1 for last stage
python3 src/5_zhtwplus.py ${DUMP} ${MAIN_OUTPUT_PATH} 1