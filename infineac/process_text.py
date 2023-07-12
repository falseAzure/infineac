import re

from tqdm import tqdm


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


def check_keywords_in_string(string: str, keywords: dict = {}) -> bool:
    if keywords == {}:
        return True

    for key, value in keywords.items():
        if string.lower().count(key) >= value:
            return True

    return False


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
