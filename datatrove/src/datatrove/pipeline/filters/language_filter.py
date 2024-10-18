from datatrove.data import Document
from datatrove.io import cached_asset_path_or_download
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.typeshelper import Languages


LANGUAGE_ID_MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"


class LanguageFilter(BaseFilter):
    name = "🌍 Language ID"
    _requires_dependencies = [("fasttext", "fasttext-wheel"), "fasteners"]

    def __init__(
        self,
        languages: tuple = (Languages.english,),
        language_threshold: float = 0.65,
        exclusion_writer: DiskWriter = None,
    ):
        """
        filters if the predicted language is not among given language or if the language score is below language
        language_threshold

        Args:
            languages: list of languages to keep
            language_threshold: language_threshold minimum score to accept a document
            exclusion_writer:
        """
        super().__init__(exclusion_writer)
        self.language_threshold = language_threshold
        self.languages = languages
        self._model = None

    @property
    def model(self):
        if not self._model:
            from fasttext.FastText import _FastText

            model_file = cached_asset_path_or_download(
                LANGUAGE_ID_MODEL_URL,
                namespace="filters",
                subfolder="language_filter",
                desc="fast-text language identifier model",
            )
            self._model = _FastText(model_file)
        return self._model

    def filter(self, doc: Document) -> bool:
        """Args:
            doc: document

        Returns:
            is_filter
        """

        language, score = self.model.predict(doc.text.replace("\n", ""), 2)
        # language label is given in the form __label__<language_id>
        #print(language)
        try:
        	language[1]# = '__label__unknown'
        except:
        	language = (language[0], '__label__unknown')
        idx = 0
        if language[1].split("__")[2] == 'zh' and language[0].split("__")[2] != 'ja' and language[0].split("__") != "ko":
            idx = 1
        language = language[idx].split("__")[2]
        #print(language)
            
        doc.metadata["language"] = language + "2" * (idx == 1)
        doc.metadata["language_score"] = score[idx].item()
        return score[idx] > self.language_threshold and language in self.languages
