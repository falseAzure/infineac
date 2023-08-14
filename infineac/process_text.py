"""
This module contains methods to process text data. It is mainly used by the
:mod:`infineac.process_event` module to process the text data of the events, e.g.
earnings calls.
"""

import re

from tqdm import tqdm

from infineac.helper import add_context_integers

FILTER_WORDS = [
    "excluding",
    "omitting",
    "except",
    "not including",
    "leaving out",
    "disregarding",
    "ignoring",
]


def get_russia_and_sanction(string: str) -> str:
    """Evaluates a string if it contains the words "russia" and "sanction" and
    returns a string accordingly."""
    string_lower = string.lower()
    if re.search(r"russia", string_lower):
        if re.search(r"sanctions", string_lower):
            return "russia & sanction"
        else:
            return "russia"
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


# def keyword_search_threshold(string: str, keywords: dict = {}) -> bool:
#     if keywords == {}:
#         return True

#     for key, value in keywords.items():
#         if string.lower().count(key) >= value:
#             return True

#     return False


def keyword_search_exclude_threshold(
    string: str,
    keywords: dict[str, int] | list[str] = {},
    filter_words: list[str] = FILTER_WORDS,
) -> bool:
    """
    Method to check if a string contains one of the `keywords` and does not
    contain a `filter_word` preceding the keyword.

    Parameters
    ----------
    string : str
        The string to be searched.
    keywords : dict[str, int] | list[str], default: {}
        Dictionary or list of `keywords`. If `keywords` is a dictionary, the key is
        the keyword and the value is the minimum number of occurrences of the
        keyword in the text.
    filter_words : list[str], default: FILTER_WORDS
        List of filter words, which must not precede the keyword

    Returns
    -------
    bool
        True if the text contains a keyword and does not contain a filter
        word. False otherwise.
    """
    if type(keywords) == list:
        keywords_orig = keywords
        keywords = {}
        for keyword in keywords_orig:
            keywords[keyword] = 1

    negative_lookbehind = "".join([r"\b(?<!" + word + "\s)" for word in filter_words])
    for key, value in keywords.items():
        keyword_pattern = "(" + key + ")"
        combined_pattern = negative_lookbehind + keyword_pattern
        found = len(re.findall(combined_pattern, string.lower(), re.IGNORECASE))
        if found >= value:
            return True

    return False


def extract_keyword_sentences_window(
    text: str,
    keywords: list[str] | dict = [],
    filter_words: list[str] = FILTER_WORDS,
    context_window_sentence: list[int] | int = 0,
    return_type: str = "list",
    nlp=None,
) -> str | list[str]:
    """
    Method to extract sentences with specific `keywords` within a text as well as
    the context surrounding this sentence.

    Parameter`
    ----------
    text : str
        The text to extract the sentences from.
    keywords : list[str] | dict, default: []
        List of `keywords` to be searched for in the text and to extract the
        sentences. If `keywords` is a dictionary, the keys are the keywords.
    filter_words : list[str], default: FILTER_WORDS
        List of filter words, which must not precede the keyword.
    context_window_sentence : list[int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a list of length 2. The first element of the list indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element the number of sentences after it. If
        only an integer is provided, the same number of sentences are extracted
        before and after the keyword. If one of the elements is -1, all
        sentences before or after the keyword are extracted. So -1 can be used
        to extract all sentences before and after the keyword, e.g. the entire
        text.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    nlp : spacy.lang, default: None
        NLP model.

    Returns
    -------
    str | list[str]
        The extracted sentences as a concatenated string or list of sentences
        (defined by `return_type`).

    Raises
    ------
    ValueError
        - If `context_window_sentence` is neither an integer nor a list of
          length 2.
    ValueError
        - If `nlp` is not a spaCy NLP model.
    """
    if nlp is None:
        raise ValueError("No spaCy NLP model provided.")
    if str == "":
        print("Empty text.")
        return ""
    if not any(keyword in text.lower() for keyword in keywords):
        print("No keyword found in text.")
        return ""
    if type(keywords) == dict:
        keywords = list(keywords.keys())
    if type(context_window_sentence) == int:
        context_window_sentence = [context_window_sentence, context_window_sentence]
    elif type(context_window_sentence) != list or len(context_window_sentence) != 2:
        raise ValueError("Context window must be an integer or a list of length 2.")

    doc = nlp(text)
    sentences = list(doc.sents)
    keyword_sent_idx = []

    for idx, sent in enumerate(sentences):
        # if any(keyword in sent.text.lower() for keyword in keywords):
        if keyword_search_exclude_threshold(sent.text.lower(), keywords, filter_words):
            keyword_sent_idx.append(idx)

    keyword_sent_idx = add_context_integers(
        keyword_sent_idx,
        context_window_sentence[0],
        context_window_sentence[1],
        0,
        len(sentences) - 1,
    )
    matching_sentences = [sentences[i] for i in keyword_sent_idx]
    if return_type == "str":
        matching_sentences = " ".join(
            [sentence.text for sentence in matching_sentences]
        )
    return matching_sentences


def extract_parts_from_paragraphs(
    paragraphs: list[str],
    keywords: list[str] | dict,
    filter_words: list[str] = FILTER_WORDS,
    context_window_sentence: list[int] | int = 0,
    subsequent_paragraphs: int = 0,
    return_type: str = "list",
    keyword_n_paragraphs_above: int = -1,
    nlp=None,
) -> str | list[list[str]]:
    """
    Function to loop through `paragraphs` and extract the sentences that
    contain a keyword. If a keyword occurs in a paragraph, the sentence
    containing it and the context surrounding it are extracted as well
    (`context_window_sentence`). Additionally, `window_subsequent` paragraphs
    are extracted.

    Parameters
    ----------
    paragraphs : list[str]
        List of `paragraphs` to loop through.
    keywords : list[str] | dict
        List of `keywords` to search for in the paragraphs. If `keywords` is a
        dictionary, the keys are the keywords.
    filter_words : list[str], default: FILTER_WORDS
        List of filter words, which must not precede the keyword.
    context_window_sentence : list[int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a list of length 2. The first element of the list indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element the number of sentences after it. If
        only an integer is provided, the same number of sentences are extracted
        before and after the keyword. If one of the elements is -1, all
        sentences before or after the keyword are extracted. So -1 can be used
        to extract all sentences before and after the keyword, e.g. the entire
        text.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    keyword_n_paragraphs_above : int, default: -1
        Number of paragraphs above the current paragraph where the keyword is
        found.
    nlp : spacy.lang, default: None
        NLP model.

    Returns
    -------
    str | list[list[str]]
        The extracted parts as a concatenated string or list of lists
        (paragraphs) of str (sentences). (defined by `return_type`).

    Raises
    ------
    ValueError
        If `return_type` is not "str" or "list".
    ValueError
        If `part_type` is not "paragraph" or "part".
    """
    if return_type not in ["str", "list"]:
        raise ValueError("output_type must be either str or list")

    if return_type == "str":
        parts_out = ""
    elif return_type == "list":
        parts_out = []

    for paragraph in paragraphs:
        # if process_text.search_keywords_in_string_exclude():
        if any(keyword in paragraph.lower() for keyword in keywords):
            keyword_n_paragraphs_above = 0
            part = extract_keyword_sentences_window(
                paragraph,
                keywords,
                filter_words,
                context_window_sentence,
                return_type,
                nlp,
            )

            if return_type == "list":
                parts_out.append(part)
            elif return_type == "str":
                parts_out += part + "\n"
        elif (
            keyword_n_paragraphs_above >= 0
            and keyword_n_paragraphs_above <= subsequent_paragraphs
        ):
            if return_type == "list":
                parts_out.append(paragraph)
            elif return_type == "str":
                parts_out += paragraph + "\n"
        if keyword_n_paragraphs_above != -1:
            keyword_n_paragraphs_above += 1
    return parts_out


def process_text_nlp(
    text_nlp: str,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = False,
    remove_currency: bool = True,
    remove_space: bool = True,
) -> list[str]:
    """
    Method to process a spaCy document. According to the parameters, the
    document is lemmatized, lowercased and stopwords, punctuation, numeric,
    currency and space tokens are removed.

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
        if remove_numeric and word.is_numeric:
            continue
        if remove_currency and word.is_currency:
            continue
        if remove_space and word.is_space:
            continue
        if lemmatize:
            word = word.lemma_
        else:
            word = word.text
        if lowercase:
            word = word.lower()
        doc.append(word)

    return doc


def process_text(
    text: str,
    nlp,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = False,
    remove_currency: bool = True,
    remove_space: bool = True,
) -> list:
    """
    Method to process a text with spaCy and an NLP model. According to the
    parameters, the document is lemmatized, lowercased and stopwords,
    punctuation, numeric, currency and space tokens are removed.

    Parameters
    ----------
    text_nlp : str
        The text document to be processed.
    nlp : spacy.lang
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

    Returns
    -------
    list[str]:
        The processed document as a list of tokens.

    Raises
    ------
    ValueError
        If `nlp` is not a spaCy NLP model.
    """
    if nlp is None:
        raise ValueError("No spaCy NLP model provided.")
    text_nlp = nlp(text)

    return process_text_nlp(
        text_nlp=text_nlp,
        lemmatize=lemmatize,
        lowercase=lowercase,
        remove_stopwords=remove_stopwords,
        remove_punctuation=remove_punctuation,
        remove_numeric=remove_numeric,
        remove_currency=remove_currency,
        remove_space=remove_space,
    )


def process_corpus(
    corpus: list[str],
    nlp,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = False,
    remove_currency: bool = True,
    remove_space: bool = True,
) -> list[list[str]]:
    """
    Method to process a corpus (list of documents/texts) with spaCy and an NLP
    model. According to the parameters, the document is lemmatized, lowercased
    and stopwords, punctuation, numeric, currency and space tokens are removed.

    Parameters
    ----------
    corpus : list[str]:
        List of texts to be processed.
    nlp : spacy.lang
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

    Returns
    -------
    list[list[str]]:
        The processed corpus as a list of lists (texts) of tokens.
    """
    print("Processing corpus with spaCy-pipeline")
    # corpus_nlp = list(nlp.pipe(corpus, batch_size=128))
    docs = []
    for doc in tqdm(
        nlp.pipe(corpus, batch_size=128), desc="Documents", total=len(corpus)
    ):
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
            )
        )

    return docs


def list_to_string(list: list, separator=" ") -> str:
    """Method to convert a list of strings to a string with a separator."""
    return separator.join(list)
