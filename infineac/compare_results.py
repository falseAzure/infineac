"""
Compare the results of the different models created by the
:mod:`infineac.pipeline` or the :mod:`infineac.topic_extractor` modules.
"""

import itertools

import polars as pl

import infineac.constants as constants
import infineac.helper as helper


def create_compare_df(dfs, suffixes):
    """
    Joins the given DataFrames(`dfs`) on the company name and year columns. The
    `suffixes` are used to rename the columns of the joined DataFrames.

    All DataFrames must contain the columns `company_name`, `year`, `topic` and
    `category` and the number of DataFrames and suffixes must be equal.

    """
    if len(dfs) != len(suffixes):
        raise ValueError("The number of dataframes and suffixes must be equal.")
    if len(dfs) == 0:
        raise ValueError("The number of dataframes must be greater than zero.")
    if len(dfs) == 1:
        return dfs[0]
    else:
        df = dfs[0]
        for i in range(1, len(dfs)):
            df = df.join(
                dfs[i].select(["company_name", "year", "topic", "category"]),
                on=["company_name", "year"],
                how="left",
                suffix=suffixes[i],
            )
        df = df.rename(
            {"topic": "topic" + suffixes[0], "category": "category" + suffixes[0]}
        )

        return df


def calculate_similarity(df: pl.DataFrame):
    """
    Calculates the intersection and union of all the given categories and
    topics and, based on this, the similarity within the categories and topics.

    The DataFrame must contain at least two topic columns and two category.
    These are then inferred by the prefix of the column names.

    Parameters
    ----------
    df : (pl.DataFrame)
        The input DataFrame containing topic and category columns.

    Returns
    -------
    pl.DataFrame
        The DataFrame with calculated similarity measures.


    Raises
    ------
    - ValueError: If no topic columns are found.
    - ValueError: If no category columns are found.
    - ValueError: If the number of topic columns and category columns are not equal.
    - ValueError: If only one topic column is found.
    - ValueError: If only one category column is found.

    Notes
    -----
    The similarity calculated is the Jaccard similarity or index: length of the
    intersection divided by the length of the union [1]_:

    .. math::

        J(A, B) = \\frac{|A \\cap B|}{|A \\cup B|}

    Normally the Jaccard similarity is calculated pairwise, i.e. for each pair
    of categories or topics. But here the Jaccard similarity is calculated in
    two ways:

    pairwise
        The Jaccard similarity is calculated pairwise and then the mean is taken.
    combined
        The Jaccard similarity is calculated for all categories or topics
        (union and intersection of all categories or topics).


    References
    ----------
    .. [1] https://en.wikipedia.org/wiki/Jaccard_index
    """
    topic_names = [name for name in df.columns if name.startswith("topic")]
    category_names = [name for name in df.columns if name.startswith("category")]

    if len(topic_names) == 0:
        raise ValueError("No topic columns found.")
    if len(category_names) == 0:
        raise ValueError("No category columns found.")
    if len(topic_names) != len(category_names):
        raise ValueError(
            "The number of topic columns and category columns must be equal."
        )
    if len(topic_names) == 1:
        raise ValueError("Only one topic column found.")
    if len(category_names) == 1:
        raise ValueError("Only one category column found.")

    df = df.with_columns(
        topic_intersection=pl.col(topic_names[0]).list.set_intersection(topic_names[1]),
        topic_union=pl.col(topic_names[0]).list.set_union(topic_names[1]),
        category_intersection=pl.col(category_names[0]).list.set_intersection(
            category_names[1]
        ),
        category_union=pl.col(category_names[0]).list.set_union(category_names[1]),
    )

    for i in range(1, len(topic_names)):
        df = df.with_columns(
            topic_intersection=pl.col("topic_intersection").list.set_intersection(
                topic_names[i]
            ),
            topic_union=pl.col("topic_union").list.set_union(topic_names[i]),
            category_intersection=pl.col("category_intersection").list.set_intersection(
                category_names[i]
            ),
            category_union=pl.col("category_union").list.set_union(category_names[i]),
        )

    df = df.with_columns(
        pl.when(pl.col("topic_union").list.len() == 0)
        .then(pl.lit(0))
        .otherwise(
            pl.col("topic_intersection").list.len() / pl.col("topic_union").list.len()
        )
        .alias("jaccard_topic_combined"),
        pl.when(pl.col("category_union").list.len() == 0)
        .then(pl.lit(0))
        .otherwise(
            pl.col("category_intersection").list.len()
            / pl.col("category_union").list.len()
        )
        .alias("jaccard_category_combined"),
    )

    jaccard_similarity_pairwise_topics = helper.jaccard_similarity_pairwise(
        topic_names, df
    )
    jaccard_similarity_pairwise_categories = helper.jaccard_similarity_pairwise(
        category_names, df
    )

    df = df.with_columns(
        pl.Series(
            name="jaccard_topic_pairwise", values=jaccard_similarity_pairwise_topics
        )
    )
    df = df.with_columns(
        pl.Series(
            name="jaccard_category_pairwise",
            values=jaccard_similarity_pairwise_categories,
        )
    )
    lst_select = (
        [
            "company_name",
            "year",
            "russia_count",
            "ukraine_count",
            "sanction_count",
            "exit_strategy",
            "stay_strategy",
            "adaptation_strategy",
        ]
        + topic_names
        + ["topic_intersection", "topic_union"]
        + category_names
        + [
            "category_intersection",
            "category_union",
            "jaccard_topic_pairwise",
            "jaccard_category_pairwise",
            "jaccard_topic_combined",
            "jaccard_category_combined",
        ]
    )

    return df.select(lst_select)


def get_strategy_list(dictionary: dict) -> list[list[str]]:
    """
    Returns a list of lists with the strategies for each row of the given
    `dict`.
    """
    strategy_list = []
    for i in range(len(dictionary[list(dictionary.keys())[0]])):
        lst_ = []
        if dictionary["exit_strategy"][i] > 0:
            lst_.append("exit")
        if dictionary["adaptation_strategy"][i] > 0:
            lst_.append("adaptation")
        if dictionary["stay_strategy"][i] > 0:
            lst_.append("stay")
        strategy_list.append(lst_)
    return strategy_list


def create_empty_count_df(
    column_names: list[str] = ["intersection", "union"]
) -> pl.DataFrame:
    """
    Creates an empty DataFrame from the cartesian product of the categories and
    the strategies with the columns `category`, `strategy` as well as the given
    `column_names`.
    """
    count = pl.DataFrame(
        list(
            itertools.product(
                list(constants.CATEGORIES_TOPICS.keys()),
                list(constants.STRATEGY_KEYWORDS.keys()) + ["no strategy"],
            )
        ),
        orient="row",
    ).rename({"column_0": "category", "column_1": "strategy"})

    for column in column_names:
        count = count.with_columns(**{column: 0})

    return count


def update_count(
    count: pl.DataFrame, categories: list[str], strategies: list[str], column: str
) -> pl.DataFrame:
    """
    Updates the given `count` DataFrame for the combination of the given
    `categories` and `strategies` in the given `column`.
    """
    if len(categories) == 0:
        return count
    if len(strategies) == 0:
        return count.with_columns(
            **{
                column: pl.when(
                    pl.col("category").is_in(categories)
                    & (pl.col("strategy") == "no strategy")
                )
                .then(pl.col(column) + 1)
                .otherwise(pl.col(column))
            }
        )
    return count.with_columns(
        **{
            column: pl.when(
                (pl.col("category").is_in(categories))
                & (pl.col("strategy").is_in(strategies))
            )
            .then(pl.col(column) + 1)
            .otherwise(pl.col(column))
        }
    )


def aggregate_results(
    df: pl.DataFrame, category_names: list[str] = ["intersection", "union"]
) -> pl.DataFrame:
    """
    Aggregates the results of the given DataFrame `df` and returns a DataFrame
    with the count of the combinations of categories and strategies. It thus
    counts the number of times a category and a strategy appear together. The
    categories are given by the `category_names`.
    """
    select_columns = ["category_" + name for name in category_names] + [
        "exit_strategy",
        "adaptation_strategy",
        "stay_strategy",
    ]

    df_dict = df.select(select_columns).to_dict(as_series=False)

    strategy_list = get_strategy_list(df_dict)
    count = create_empty_count_df()

    for column in category_names:
        for i, categories in enumerate(df_dict["category_" + column]):
            strategies = strategy_list[i]
            if len(categories) == 0:
                continue
            count = update_count(count, categories, strategies, column)

    return count
