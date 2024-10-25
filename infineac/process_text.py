"""
Contains methods to process text data. The methods are used to extract passages
from text data, e.g. earnings calls, that contain specific keywords.

Examples
--------
>>> import infineac.process_text as process_text
>>> import spacy_stanza
>>> nlp = spacy_stanza.load_pipeline("en", processors="tokenize, lemma")
>>> nlp_stanza.add_pipe('sentencizer')
>>> nlp = "data/transcripts/"
>>> files = list(Path(PATH_DIR).rglob("*.xml"))
>>> events = file_loader.load_files_from_xml(files)
>>> keywords = {"russia": 1, "ukraine": 1}
>>> process_text.extract_passages_from_events(
    events=events,
    keywords=keywords,
    nlp_model=nlp)

Notes
-----
    It is mainly used by the :mod:`infineac.process_event` module to process
    the text data of the events, e.g. earnings calls.
"""


import random
import re

import polars as pl
from tqdm import tqdm

import infineac.constants as constants
from infineac.helper import add_context_integers


def get_russia_and_sanction(string: str) -> str:
    """Evaluates a string if it contains the words "russia" and "sanction" and
    returns a string accordingly."""
    string_lower = string.lower()
    if re.search(r"russia|ukraine", string_lower):
        if re.search(r"sanctions", string_lower):
            return "russia/ukraine & sanction"
        elif re.search(r"russia", string_lower) and re.search(r"ukraine", string_lower):
            return "russia & ukraine"
        elif re.search(r"russia", string_lower):
            return "russia"
        elif re.search(r"ukraine", string_lower):
            return "ukraine"
    if re.search(r"sanction", string_lower):
        return "sanction"
    else:
        return "none"


def get_elections(string: str) -> str:
    """Evaluates a string if it contains the words "election" and "presidential
    election" and returns a string accordingly."""
    string_lower = string.lower()
    if re.search(r"presidential election", string_lower):
        return "presidential election"
    if re.search(r" election", string_lower):
        return "election"
    else:
        return "none"


def combine_adjacent_sentences(
    sentence_ids: list[int], sentences: list[str]
) -> list[str]:
    """Joins `sentences` that are adjacent based on their `sentence_ids`."""
    joined_sentences = []
    current_sentence = sentences[sentence_ids[0]]
    for i in range(1, len(sentence_ids)):
        if sentence_ids[i] == sentence_ids[i - 1] + 1:
            current_sentence += " " + sentences[sentence_ids[i]]
        else:
            joined_sentences.append(current_sentence)
            current_sentence = sentences[sentence_ids[i]]

    joined_sentences.append(current_sentence)
    return joined_sentences


def keyword_threshold_search_exclude_mod(
    string: str,
    keywords: dict[str, int] | list[str] = {},
    modifier_words: list[str] = constants.MODIFIER_WORDS,
) -> bool:
    """
    Checks if a string contains one of the `keywords` and does not
    contain a `modifier_word` preceding the keyword.

    Parameters
    ----------
    string : str
        The string to be searched.
    keywords : dict[str, int] | list[str], default: {}
        Dictionary or list of `keywords`. If `keywords` is a dictionary, the key is
        the keyword and the value is the minimum number of occurrences of the
        keyword in the text.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.

    Returns
    -------
    bool
        True if the text contains a keyword and does not contain a modifier
        word preceding it. False otherwise.
    """
    if type(keywords) is list:
        keywords_orig = keywords
        keywords = {}
        for keyword in keywords_orig:
            keywords[keyword] = 1

    negative_lookbehind = "".join([r"\b(?<!" + word + "\s)" for word in modifier_words])
    for key, value in keywords.items():
        keyword_pattern = "(" + key + ")"
        if len(modifier_words) > 0:
            combined_pattern = negative_lookbehind + keyword_pattern
        else:
            combined_pattern = " " + keyword_pattern
        found = len(re.findall(combined_pattern, string.lower(), re.IGNORECASE))
        if found >= value:
            return True

    return False


def extract_keyword_sentences_window(  # noqa: C901
    text: str,
    keywords: list[str] | dict[str, int],
    nlp_model,
    modifier_words: list[str] = constants.MODIFIER_WORDS,
    context_window_sentence: tuple[int, int] | int = 0,
    join_adjacent_sentences: bool = True,
    return_type: str = "list",
) -> str | list[str]:
    """
    Extracts sentences with specific `keywords` within a text as well as the
    context surrounding this sentence.

    Parameter`
    ----------
    text : str
        The text to extract the sentences from.
    keywords : list[str] | dict[str, int], default: []
        List of `keywords` to be searched for in the text and to extract the
        sentences. If `keywords` is a dictionary, the keys are the keywords.
    nlp_model : spacy.lang
        NLP model.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
            Whether to join adjacent sentences or leave them as individual. If
            `context_window_sentence` > 0, this parameter is automatically set
            to `True`.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"

    Returns
    -------
    str | list[str]
        The extracted sentences as a concatenated string or list of passages
        (defined by `return_type`).

    Raises
    ------
    ValueError
        - If `context_window_sentence` is neither an integer nor a list of
          length 2.
    """
    if str == "":
        print("Empty text.")
        return ""
    if not any(keyword in text.lower() for keyword in keywords):
        print("No keyword found in text.")
        return ""
    if type(keywords) is dict:
        keywords = list(keywords.keys())
    if type(context_window_sentence) is int:
        context_window_sentence = [context_window_sentence, context_window_sentence]
    elif type(context_window_sentence) is not list or len(context_window_sentence) != 2:
        raise ValueError("Context window must be an integer or a list of length 2.")
    if context_window_sentence[0] > 0 or context_window_sentence[1] > 0:
        join_adjacent_sentences = True
    doc = nlp_model(text.strip())
    sentences = list(doc.sents)
    # print(sentences)
    keyword_sent_idx = []
    # print(len(sentences))

    for idx, sent in enumerate(sentences):
        # if any(keyword in sent.text.lower() for keyword in keywords):
        if keyword_threshold_search_exclude_mod(
            sent.text.lower().strip(), keywords, modifier_words
        ):
            keyword_sent_idx.append(idx)
    keyword_sent_idx = add_context_integers(
        lst=keyword_sent_idx,
        m=context_window_sentence[0],
        n=context_window_sentence[1],
        min_int=0,
        max_int=len(sentences) - 1,
    )
    # print(keyword_sent_idx)

    sentences_str = [sentence.text.strip() for sentence in sentences]
    if join_adjacent_sentences and len(keyword_sent_idx) > 1:
        matching_sentences = combine_adjacent_sentences(keyword_sent_idx, sentences_str)
    else:
        matching_sentences = [sentences_str[i] for i in keyword_sent_idx]

    # print(matching_sentences, "\n")

    if return_type == "str":
        matching_sentences = " ".join(
            [sentence.text for sentence in matching_sentences]
        )
    return matching_sentences


def extract_passages_from_paragraphs(  # noqa: C901
    paragraphs: list[str],
    keywords: list[str] | dict[str, int],
    nlp_model,
    modifier_words: list[str] = constants.MODIFIER_WORDS,
    context_window_sentence: tuple[int, int] | int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    return_type: str = "list",
    keyword_n_paragraphs_above: int = -1,
) -> str | list[list[str]]:
    """
    Loops through `paragraphs` and extracts the sentences that contain a
    keyword.

    If a keyword occurs in a paragraph, the sentence containing it and the
    context surrounding it are extracted as well (`context_window_sentence`).
    Additionally, `window_subsequent` paragraphs are extracted.

    Parameters
    ----------
    paragraphs : list[str]
        List of `paragraphs` to loop through.
    keywords : list[str] | dict[str, int]
        List of `keywords` to search for in the paragraphs. If `keywords` is a
        dictionary, the keys are the keywords.
    nlp_model : spacy.lang
        NLP model.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
            Whether to join adjacent sentences or leave them as individual. If
            `context_window_sentence` > 0, this parameter is automatically set
            to `True`.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    keyword_n_paragraphs_above : int, default: -1
        Number of paragraphs above the current paragraph where the keyword is
        found.

    Returns
    -------
    str | list[list[str]]
        The extracted passages as a concatenated string or list of paragraphs
        (lists) of passages (str). (defined by `return_type`).

    Raises
    ------
    ValueError
        If `return_type` is not "str" or "list".
    """
    if return_type not in ["str", "list"]:
        raise ValueError("output_type must be either str or list")

    if return_type == "str":
        passages_out = ""
    elif return_type == "list":
        passages_out = []

    for paragraph in paragraphs:
        # if process_text.search_keywords_in_string_exclude():
        if any(keyword in paragraph.lower() for keyword in keywords):
            keyword_n_paragraphs_above = 0
            passage = extract_keyword_sentences_window(
                text=paragraph,
                keywords=keywords,
                nlp_model=nlp_model,
                modifier_words=modifier_words,
                context_window_sentence=context_window_sentence,
                join_adjacent_sentences=join_adjacent_sentences,
                return_type=return_type,
            )

            if return_type == "list":
                passages_out.append(passage)
            elif return_type == "str":
                passages_out += passage + "\n"
        elif (
            keyword_n_paragraphs_above != -1
            and keyword_n_paragraphs_above <= subsequent_paragraphs
        ):
            if return_type == "list":
                passages_out.append([paragraph])
            elif return_type == "str":
                passages_out += paragraph + "\n"
        if keyword_n_paragraphs_above != -1:
            keyword_n_paragraphs_above += 1
    return passages_out


def keyword_threshold_search_include_mod(
    string: str,
    keywords: list[str] = [],
    modifier_words: list[str] = constants.MODIFIER_WORDS,
) -> bool:
    """
    Checks if a string contains one of the `keywords` and contains a `modifier_word`
    preceding the keyword. Used to obtain the sentences, that are filtered out
    by :func:`keyword_search_exclude_threshold`.

    Parameters
    ----------
    string : str
        The string to be searched.
    keywords : list[str], default: []
        List of `keywords` to be searched for in the text and to extract the
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must precede the keyword.

    Returns
    -------
    bool
        True if the text contains a keyword and a modifier word preceding it.
        False otherwise.
    """
    modifier_pattern = r"(?:" + "|".join(modifier_words) + r")"
    keyword_pattern = r"(?:" + "|".join(keywords) + r")"

    pattern = rf"{modifier_pattern} {keyword_pattern}"
    return bool(re.search(pattern, string, re.IGNORECASE))


def extract_keyword_sentences_preceding_mod(
    text: str,
    keywords: list[str],
    nlp_model,
    modifier_words: list[str] = constants.MODIFIER_WORDS,
) -> str | list[str]:
    """
    Extracts sentences with specific `keywords` and a `modifier_word` preceding
    it. Used to obtain the sentences, that are filtered out by
    :func:`keyword_search_exclude_threshold` and
    :func:`extract_keyword_sentences_window` as well as all functions that use
    one of them. Is called by
    :func:`infineac.process_event.excluded_sentences_by_mod_words`

    Parameter
    ----------
    text : str
        The text to extract the sentences from.
    keywords : list[str], default: []
        List of `keywords` to be searched for in the text and to extract the
        sentences.
    nlp_model : spacy.lang
        NLP model.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words` which must precede the keyword.

    Returns
    -------
    str | list[str]
        The extracted sentences as a list of passages.
    """
    if text == "":
        print("Empty text.")
        return ""
    doc = nlp_model(text)
    sentences = list(doc.sents)
    keyword_sent_idx = []

    for idx, sent in enumerate(sentences):
        if keyword_threshold_search_include_mod(
            string=sent.text.lower(), keywords=keywords, modifier_words=modifier_words
        ):
            keyword_sent_idx.append(idx)

    sentences_str = [sentence.text for sentence in sentences]
    matching_sentences = [sentences_str[i] for i in keyword_sent_idx]

    return matching_sentences


def extract_keyword_sentences_preceding_mod_nlp(
    doc: str,
    keywords: list[str],
    modifier_words: list[str] = constants.MODIFIER_WORDS,
) -> str | list[str]:
    """
    Extracts sentences with specific `keywords` and a `modifier_word` preceding
    it. Used to obtain the sentences, that are filtered out by
    :func:`keyword_search_exclude_threshold` and
    :func:`extract_keyword_sentences_window` as well as all functions that use
    one of them. Is called by
    :func:`infineac.process_event.excluded_sentences_by_mod_words`

    Parameter
    ----------
    doc : str
        The spaCy document to be processed.
    keywords : list[str], default: []
        List of `keywords` to be searched for in the text and to extract the
        sentences.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words` which must precede the keyword.

    Returns
    -------
    str | list[str]
        The extracted sentences as a list of passages.
    """
    sentences = list(doc.sents)
    keyword_sent_idx = []

    for idx, sent in enumerate(sentences):
        if keyword_threshold_search_include_mod(
            sent.text.lower(), keywords, modifier_words
        ):
            keyword_sent_idx.append(idx)

    sentences_str = [sentence.text for sentence in sentences]
    matching_sentences = [sentences_str[i] for i in keyword_sent_idx]

    return matching_sentences


def process_text_nlp(  # noqa: C901
    text_nlp: str,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = True,
    remove_currency: bool = True,
    remove_space: bool = True,
    remove_additional_words_part: list[str] = [],
    remove_additional_words_whole: list[str] = [],
) -> list[str]:
    """
    Processes a spaCy document.

    According to the parameters, the document is `lemmatized`, `lowercased` and
    `stopwords`, `additional_words`, `punctuation`, `numeric`, `currency` and
    `space` tokens as well as `additional_words_part` and
    `additional_words_whole` are removed from the document.

    Parameters
    ----------
    text_nlp : str
        The spaCy document to be processed.
    lemmatize : bool, default: True
        If document should be lemmatized.
    lowercase : bool, default: True
        If document should be lowercased.
    remove_stopwords : bool, default: True
        If stopwords should be removed from document.
    remove_punctuation : bool, default: True
        If punctuation should be removed from document.
    remove_numeric : bool, default: False
        If numerics should be removed from document.
    remove_currency : bool, default: True
        If currency symbols should be removed from document.
    remove_space : bool, default: True
        If spaces should be removed from document.
    remove_additional_words_part : list[str], default: []
        List of additional words to be removed from the document. These words
        can be part of a another word.
    remove_additional_words_whole : list[str], default: []
        List of additional words to be removed from the document. These words
        must be a whole, individual word.

    Returns
    -------
    list[str]
        The processed document as a list of tokens.
    """
    doc = []
    for word in text_nlp:
        if remove_stopwords and word.is_stop:
            continue
        if remove_punctuation and word.is_punct:
            continue
        if remove_numeric and word.is_digit:
            continue
        if remove_currency and word.is_currency:
            continue
        if remove_space and word.is_space:
            continue
        if contains_stopword(word.lemma_, remove_additional_words_part):
            continue
        if word.text in remove_additional_words_whole:
            continue
        if lemmatize:
            word = word.lemma_
        else:
            word = word.text
        if lowercase:
            word = word.lower()
        doc.append(word)

    return doc


def starts_with_additional_word(word: str, additional_words: list[str]) -> bool:
    """Checks if a word starts with an `additional_word`."""
    for additional_word in additional_words:
        if word.startswith(additional_word):
            return True
    return False


def contains_stopword(word: str, stopwords: list[str], only_start: bool = True) -> bool:
    """Checks if a word contains a `stopword`."""
    if stopwords == []:
        return False
    pattern = r"(?:" + "|".join(stopwords) + r")"  # \b would be word boundary
    if only_start is True:
        pattern = r"\b" + pattern
    return bool(re.search(pattern, word, re.IGNORECASE))


def process_text(
    text: str,
    nlp_model,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = True,
    remove_currency: bool = True,
    remove_space: bool = True,
    remove_additional_words_part: list[str] = [],
    remove_additional_words_whole: list[str] = [],
) -> list:
    """
    Processes a text with spaCy and an NLP model.

    According to the parameters, the document is `lemmatized`, `lowercased` and
    `stopwords`, `additional_words`, `punctuation`, `numeric`, `currency` and
    `space` tokens as well as `additional_words_part` and
    `additional_words_whole` are removed from the document.

    Parameters
    ----------
    text_nlp : str
        The text document to be processed.
    nlp_model : spacy.lang
        The spaCy NLP model.
    lemmatize : bool, default: True
        If document should be lemmatized.
    lowercase : bool, default: True
        If document should be lowercased.
    remove_stopwords : bool, default: True
        If stopwords should be removed from document.
    remove_punctuation : bool, default: True
        If punctuation should be removed from document.
    remove_numeric : bool, default: False
        If numerics should be removed from document.
    remove_currency : bool, default: True
        If currency symbols should be removed from document.
    remove_space : bool, default: True
        If spaces should be removed from document.
    remove_additional_words_part : list[str], default: []
        List of additional words to be removed from the document. These words
        can be part of a another word.
    remove_additional_words_whole : list[str], default: []
        List of additional words to be removed from the document. These words
        must be a whole, individual word.

    Returns
    -------
    list[str]:
        The processed document as a list of tokens.
    """
    text_nlp = nlp_model(text)

    return process_text_nlp(
        text_nlp=text_nlp,
        lemmatize=lemmatize,
        lowercase=lowercase,
        remove_stopwords=remove_stopwords,
        remove_punctuation=remove_punctuation,
        remove_numeric=remove_numeric,
        remove_currency=remove_currency,
        remove_space=remove_space,
        remove_additional_words_part=remove_additional_words_part,
        remove_additional_words_whole=remove_additional_words_whole,
    )


def process_corpus(
    corpus: list[str],
    nlp_model,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = True,
    remove_currency: bool = True,
    remove_space: bool = True,
    remove_additional_words_part: list[str] = [],
    remove_specific_stopwords: list[list[str]] = [],
) -> list[list[str]]:
    """
    Processes a corpus (list of documents/texts) with spaCy and an NLP
    model.

    According to the parameters, the document is `lemmatized`, `lowercased` and
    `stopwords`, `additional_words`, `punctuation`, `numeric`, `currency` and
    `space` tokens as well as `names`, `strategies` and
    `additional_words_whole` are removed from the corpus.

    Parameters
    ----------
    corpus : list[str]:
        List of texts to be processed.
    nlp_model : spacy.lang
        The spaCy NLP model.
    lemmatize : bool, default: True
        If document should be lemmatized.
    lowercase : bool, default: True
        If document should be lowercased.
    remove_stopwords : bool, default: True
        If stopwords should be removed from document.
    remove_punctuation : bool, default: True
        If punctuation should be removed from document.
    remove_numeric : bool, default: False
        If numerics should be removed from document.
    remove_currency : bool, default: True
        If currency symbols should be removed from document.
    remove_space : bool, default: True
        If spaces should be removed from document.
    remove_additional_words : list[str], default: True
        List of additional words to be removed from the document. These words
        can be part of a another word.
    remove_specific_stopwords : list[list[str]], default: []
        List of lists of stopwords to be removed from the document. Each list
        of stopwords corresponds to a document in the corpus.

    Returns
    -------
    list[list[str]]:
        The processed corpus as a list of lists (texts) of tokens.
    """
    print("Processing corpus with spaCy-pipeline")
    # corpus_nlp = list(nlp.pipe(corpus, batch_size=128))
    docs = []

    for idx_doc, doc in enumerate(
        tqdm(
            nlp_model.pipe(corpus, batch_size=128), desc="Documents", total=len(corpus)
        )
    ):
        if remove_specific_stopwords:
            remove_additional_words_whole = remove_specific_stopwords[idx_doc]
        docs.append(
            process_text_nlp(
                text_nlp=doc,
                lemmatize=lemmatize,
                lowercase=lowercase,
                remove_stopwords=remove_stopwords,
                remove_punctuation=remove_punctuation,
                remove_numeric=remove_numeric,
                remove_currency=remove_currency,
                remove_space=remove_space,
                remove_additional_words_part=remove_additional_words_part,
                remove_additional_words_whole=remove_additional_words_whole,
            )
        )
    return docs


def list_to_string(list: list, separator=" ") -> str:
    """Converts a list of strings to a string with a separator. Is used to
    convert the list of tokens from :func:`process_corpus` back to a string."""
    return separator.join(list)


def get_strategies(
    lst: list = [],
    strategy_keywords: dict[str, list[str]] = constants.STRATEGY_KEYWORDS,
    modifier_words: list[str] = constants.MODIFIER_WORDS_STRATEGY,
    dataframe: pl.DataFrame = None,
) -> dict[str, list[bool]] | pl.DataFrame:
    """Searches for the `strategy_keywords` in a list of strings or DataFrame
    and returns a boolean list or DataFrame with the results (for each strategy).
    """
    if type(dataframe) is pl.dataframe.frame.DataFrame:
        lst = list(dataframe["text"].to_list())
    strategies = {}
    for strategy in tqdm(
        strategy_keywords.keys(), desc="Strategies", total=len(strategy_keywords.keys())
    ):
        keywords = strategy_keywords[strategy]
        list_strategy = []
        for text in lst:
            list_strategy.append(
                keyword_threshold_search_exclude_mod(text, keywords, modifier_words)
            )
        strategies[strategy] = list_strategy

    if type(dataframe) is pl.dataframe.frame.DataFrame:
        for strategy in strategies.keys():
            dataframe = dataframe.with_columns(
                pl.Series(
                    name=strategy + "_strategy",
                    values=strategies[strategy],
                )
            )
            # dataframe[strategy] = pl.Series(strategies[strategy])
        return dataframe

    return strategies


def sample_strategies(dataframe: pl.DataFrame, k=10) -> list[str]:
    """Samples the strategies from the dataframe."""
    exit_list = list(dataframe.filter(pl.col("exit_strategy").is_in(True))["text"])
    stay_list = list(dataframe.filter(pl.col("stay_strategy").is_in(True))["text"])
    adaptation_list = list(
        dataframe.filter(pl.col("adaptation_strategy").is_in(True))["text"]
    )

    return (
        random.sample(exit_list, k=min(k, len(exit_list)))
        + random.sample(stay_list, k=min(k, len(stay_list)))
        + random.sample(adaptation_list, k=min(k, len(adaptation_list)))
    )


def strategy_keywords_tolist(
    strategy_keywords: dict[str, list[str]] = constants.STRATEGY_KEYWORDS
) -> list[str]:
    """Converts the dictionary of `strategy_keywords` to a list of keywords."""
    keywords = []
    for strategy in strategy_keywords.keys():
        keywords += strategy_keywords[strategy]
    return keywords


def remove_sentences_under_threshold(
    corpus: list[str], threshold: int = 1
) -> list[str] and list[int]:
    """Removes sentences from a `corpus` that only contain `threshold` words or
    less. Returns a transformed corpus as well as a list of indices that
    indicate the original position of the sentences in the corpus."""
    corpus_cleaned = []
    mapping = []
    i = 0
    for el in corpus:
        length = len(el.split(" "))
        if length <= threshold:
            mapping.append(-1)
        else:
            corpus_cleaned.append(el)
            mapping.append(i)
            i += 1
    return corpus_cleaned, mapping
