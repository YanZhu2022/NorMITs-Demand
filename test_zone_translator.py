# -*- coding: utf-8 -*-
"""
    Module containing tests for zone_translator module, tests
    are setup to use pytest.
"""

##### IMPORTS #####
# Standard imports
from typing import Tuple

# Third party imports
import numpy as np
import pandas as pd
import pytest

# Local imports
from zone_translator import translate_matrix


##### CLASSES #####
class TestTranslateMatrix:
    """Tests for the `translate_matrix` function. """

    NOHAM_2_NORMS = pd.DataFrame({"noham": [1, 2, 3], "norms": [1, 1, 2]})
    NORMS_2_NOHAM = pd.DataFrame(
        {"norms": [1, 1, 2], "noham": [1, 2, 3], "split": [0.8, 0.2, 1]}
    )
    NORMS_MAT = pd.DataFrame(
        np.array([[2.3, 0.1], [7.4, 9.4]]),
        columns=[1, 2],
        index=[1, 2],
    )
    NOHAM_MAT = pd.DataFrame(
        np.array([[1.472, 0.368, 0.08], [0.368, 0.092, 0.02], [5.92, 1.48, 9.4]]),
        columns=[1, 2, 3],
        index=[1, 2, 3],
    )
    PARAMETER_STR = "mat, lookup, lookup_cols, split_col, expected"
    PARAMETERS = [
        (NOHAM_MAT, NOHAM_2_NORMS, ["noham", "norms"], None, NORMS_MAT),
        (NORMS_MAT, NORMS_2_NOHAM, ["norms", "noham"], "split", NOHAM_MAT),
    ]

    @staticmethod
    @pytest.mark.parametrize(PARAMETER_STR, PARAMETERS)
    def test_totals_equal(
        mat: pd.DataFrame,
        lookup: pd.DataFrame,
        lookup_cols: Tuple[str, str],
        split_col: str,
        expected: pd.DataFrame,
    ):
        """Test that the translation doesn't change the matrix total. """
        test = translate_matrix(mat, lookup, lookup_cols, split_column=split_col)
        assert np.sum(test.values) == np.sum(expected.values)

    @staticmethod
    @pytest.mark.parametrize(PARAMETER_STR, PARAMETERS)
    def test_square_matrix(
        mat: pd.DataFrame,
        lookup: pd.DataFrame,
        lookup_cols: Tuple[str, str],
        split_col: str,
        expected: pd.DataFrame,
    ):
        """Test that the translations works correctly between two test matrices. """
        test = translate_matrix(mat, lookup, lookup_cols, split_column=split_col)
        pd.testing.assert_frame_equal(test, expected)

    @staticmethod
    @pytest.mark.parametrize(PARAMETER_STR, PARAMETERS)
    def test_list_matrix(
        mat: pd.DataFrame,
        lookup: pd.DataFrame,
        lookup_cols: Tuple[str, str],
        split_col: str,
        expected: pd.DataFrame,
    ):
        """Test that the translation works correctly for list format matrices. """
        # Convert square matrices to list
        list_matrices = []
        for m in (mat, expected):
            m = m.melt(ignore_index=False).reset_index()
            m.columns = ["o", "d", "value"]
            list_matrices.append(m.sort_values(["o", "d"]).reset_index(drop=True))

        test = translate_matrix(
            list_matrices[0],
            lookup,
            lookup_cols,
            zone_cols=["o", "d"],
            split_column=split_col,
            square_format=False,
        )
        # 15th decimal may be different due to float division
        np.testing.assert_almost_equal(
            test["value"].sum(), list_matrices[1]["value"].sum(), 14
        )
        pd.testing.assert_frame_equal(test, list_matrices[1])
