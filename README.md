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

If you like our work, please cite
```
@misc{lin2024finewebzhtwscalablecurationtraditional,
      title={FineWeb-zhtw: Scalable Curation of Traditional Chinese Text Data from the Web}, 
      author={Cheng-Wei Lin and Wan-Hsuan Hsieh and Kai-Xin Guan and Chan-Jan Hsu and Chia-Chen Kuo and Chuan-Lin Lai and Chung-Wei Chung and Ming-Jen Wang and Da-Shan Shiu},
      year={2024},
      eprint={2411.16387},
      archivePrefix={arXiv},
      primaryClass={cs.CL},
      url={https://arxiv.org/abs/2411.16387}, 
}
```

