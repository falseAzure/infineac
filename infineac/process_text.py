import re

from tqdm import tqdm

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
    """
    Evaluates a string if it contains the words "russia" and "sanction" and
    returns a string accordingly.

    Args:
        string (str): String to be evaluated.

    Returns:
        str: Returns "russia & sanction" if the string contains both words,
        "russia" if it contains only "russia", "sanction" if it contains only
        "sanction" and "none" if it contains neither.
    """
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
    """
    Evaluates a string if it contains the words "election" and "presidential
    election" and returns a string accordingly.

    Args:
        string (str): String to be evaluated.

    Returns:
        str: Returns "presidential election" if the string contains both words,
        "election" if it contains only "election", "none" if it contains
        neither.
    """
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
    keywords: dict | list = {},
    filter_words: list = FILTER_WORDS,
):
    """
    Method to check if a text contains a keyword and does not contain a filter
    word preceding the keyword.

    Args:
        string (str): The text to be searched.
        keywords (dict | list, optional): Dictionary or list of keywords. If
        keywords is a dictionary, the key is the keyword and the value is the
        minimum number of occurrences of the keyword in the text. Defaults to {}.
        filter_words (list, optional): List of filter words, which must not
        precede the keyword. Defaults to FILTER_WORDS.

    Returns:
        _type_: _description_
    """
    if type(keywords) == "list":
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


def get_sentences_after_keywords(text: str, keywords: list = [], nlp=None) -> str:
    """
    Method to extract the sentences after a keyword as well as the sentence
    the keyword was found.

    Args:
        text (str): The text to extract the sentences from.
        keywords (list, optional): The keywords starting from which the
        sentences are extracted. Defaults to {}.
        nlp (Spacy.lang, optional): NLP model. Defaults to None.

    Returns:
        str: The extracted sentences.
    """
    if nlp is None:
        return None
    if str == "":
        return ""
    if not any(keyword in text.lower() for keyword in keywords):
        return ""

    doc = nlp(text)
    sentences = list(doc.sents)
    for idx, sent in enumerate(sentences):
        if any(keyword in sent.text.lower() for keyword in keywords):
            start_idx = idx
            break
    matching_sentences = sentences[start_idx:]
    part = " ".join([sentence.text for sentence in matching_sentences])
    return part


def process_text_nlp(
    text_nlp: str,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = False,
    remove_currency: bool = True,
    remove_space: bool = True,
) -> list:
    """
    Method to process a spaCy document. According to the parameters, the
    document is lemmatized, lowercased and stopwords, punctuation, numeric,
    currency and space tokens are removed.

    Args:
        text_nlp (str): The spaCy document to be processed.
        lemmatize (bool, optional): If document should be lemmatized.
        Defaults to True.
        lowercase (bool, optional): If document should be lowercased.
        Defaults to True.
        remove_stopwords (bool, optional): If stopwords should be removed from
        document. Defaults to True.
        remove_punctuation (bool, optional): If punctuation should be removed from
        document. Defaults to True.
        remove_numeric (bool, optional): If numerics should be removed from
        document. Defaults to False.
        remove_currency (bool, optional): If currency symbols should be removed
        from document. Defaults to True.
        remove_space (bool, optional): If spaces should be removed from
        document. Defaults to True.

    Returns:
        list: The processed document as a list of tokens.
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

    Args:
        text (str): The text to be processed.
        nlp (spacy.lang): spaCy language model.
        lemmatize (bool, optional): If document should be lemmatized.
        Defaults to True.
        lowercase (bool, optional): If document should be lowercased.
        Defaults to True.
        remove_stopwords (bool, optional): If stopwords should be removed from
        document. Defaults to True.
        remove_punctuation (bool, optional): If punctuation should be removed from
        document. Defaults to True.
        remove_numeric (bool, optional): If numerics should be removed from
        document. Defaults to False.
        remove_currency (bool, optional): If currency symbols should be removed
        from document. Defaults to True.
        remove_space (bool, optional): If spaces should be removed from
        document. Defaults to True.

    Returns:
        list: The processed document as a list of tokens.
    """
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
    corpus: list,
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
    Method to process a corpus (list of documents/texts) with spaCy and an NLP
    model. According to the parameters, the document is lemmatized, lowercased
    and stopwords, punctuation, numeric, currency and space tokens are removed.

    Args:
        corpus (list): List of texts to be processed.
        nlp (spacy.lang): spaCy language model.
        lemmatize (bool, optional): If document should be lemmatized.
        Defaults to True.
        lowercase (bool, optional): If document should be lowercased.
        Defaults to True.
        remove_stopwords (bool, optional): If stopwords should be removed from
        document. Defaults to True.
        remove_punctuation (bool, optional): If punctuation should be removed from
        document. Defaults to True.
        remove_numeric (bool, optional): If numerics should be removed from
        document. Defaults to False.
        remove_currency (bool, optional): If currency symbols should be removed
        from document. Defaults to True.
        remove_space (bool, optional): If spaces should be removed from
        document. Defaults to True.

    Returns:
        list: The processed corpus as a list of lists of tokens.
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


def list_to_string(list: list, seperator=" ") -> str:
    return seperator.join(list)
