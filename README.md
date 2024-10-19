# Fineweb-zhtw

## Installation
Clone repo with submodules
```bash
git clone --recurse-submodules https://github.com/mtkresearch/fineweb-zhtw.git
```

Install requirements
```bash
pip install -r requirements.txt
```

## To Run
prepare data for pipeline demonstration (download 2 WARC files)
```bash
bash scripts/get_data_example.sh
```

run cleaning
```bash
bash scripts/map_warc_to_zhtw_text.sh
```
Results would be under `data/parsed/{DUMP}/5_zhtwplus/output`



