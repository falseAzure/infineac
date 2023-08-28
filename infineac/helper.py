"""
This module contains helper functions for the infineac package.
"""

import copy
import lzma
import re

import dill as pickle
import lz4.frame


def add_context_integers(
    lst: list[int],
    m: int,
    n: int,
    min_int: int = -1,
    max_int: int = -1,
) -> list[int]:
    """
    Method that adds to a list of positive integers the `n` subsequent and `m`
    prior integers of each integer in the list. Only adds the integers if they
    are not already in the list.

    Parameters
    ----------
    lst : list[int]
        List of integers.
    m : int
        `m` prior integers of each element to add to list. If `m` = -1 all prior
        integers (until min) are added. If `m` = -1 and `min_int` = -1 all prior
        integers until the minimum integer in the list are added.
    n : int
        `n` subsequent integers of each element to add to list. If `n` = -1 all
        subsequent integers (until max) are added. If `n`= -1 and `max_int` =
        -1 all subsequent integers until the maximum integer in the list are
        added.
    min_int : int, default: -1 (no limit)
        Minimum integer to add.
    max_int : int, default: -1 (no limit)
        Maximum integer to add.

    Returns
    -------
    list[int]
        Extended list of integers.

    Raises
    ------
    ValueError
        If `m` or `n` are smaller than -1.
    ValueError
        If `lst` contains negative integers.

    Examples
    --------
    >>> add_context_integers([3, 4, 15], 1, 1, -1, -1)
    [2, 3, 4, 5, 14, 15, 16]
    >>> add_context_integers([3, 4, 15], 1, 2, -1, -1)
    [2, 3, 4, 5, 6, 14, 15, 16, 17]
    >>> add_context_integers([3, 4, 15], 3, 1, -1, -1)
    [0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16]
    >>> add_context_integers([3, 4, 15], -1, 1, -1, -1)
    [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    >>> add_context_integers([3, 4, 15], 1, -1, -1, -1)
    [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    >>> add_context_integers([3, 4, 15], -1, -1, 0, 18)
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
    """
    if n < -1 or m < -1:
        raise ValueError("n and m must be greater than -1.")
    if not all(num >= 0 for num in lst):
        raise ValueError("All integers in lst must be positive.")
    if type(n) != int or type(m) != int:
        raise TypeError("n and m must be integers.")

    if n == 0 and m == 0:
        return lst

    lst_ = copy.deepcopy(lst)
    new_elements = []

    if m == -1:
        if min_int == -1:
            min_int_ = min(lst_)
        else:
            min_int_ = min_int
        for i in range(min_int_, max(lst_)):
            new_elements.append(i)
        m_ = 0
    else:
        m_ = m

    if n == -1:
        if max_int == -1:
            max_int_ = max(lst_)
        else:
            max_int_ = max_int
        for i in range(min(lst_), max_int_ + 1):
            new_elements.append(i)
        n_ = 0
    else:
        n_ = n

    for num in lst_:
        for i in range(-m_, n_ + 1):
            next_num = num + i
            if (next_num >= max_int and max_int != -1) or next_num < min_int:
                continue
            new_elements.append(next_num)

    new_elements = list(set(new_elements + lst_))
    return new_elements


def fill_list(lst: list, min: int, max: int):
    """Method to fill a list with integers from min to max."""
    lst_ = copy.deepcopy(lst)
    for i in range(min, max):
        if i not in lst_:
            lst_.append(i)
    return sorted(lst_)


def save_data(data: dict, name: str = "events", compression: str | bool = False):
    """Method to save data. Compression can be lzma, lz4 or False."""
    if compression == "lzma":
        with lzma.open(name + ".xz", "wb") as f:
            pickle.dump(data, f)
    if compression == "lz4" or compression:
        with lz4.frame.open(name + ".lz4", "wb") as f:
            pickle.dump(data, f)
    else:
        with open(name + ".pickle", "wb") as f:
            pickle.dump(data, f)


def load_data(name: str):
    """Method to load data. Compression can be lzma, lz4 or False."""
    compression = re.sub(r".*\.", "", name)
    if compression == "lzma":
        with lzma.open(name, "rb") as f:
            data = pickle.load(f)
    if compression == "lz4" or compression:
        with lz4.frame.open(name, "rb") as f:
            data = pickle.load(f)
    if compression == "pickle" or not compression:
        with open(name, "rb") as f:
            data = pickle.load(f)
    return data
