"""Example dagster repository containing a data preprocessing pipeline"""

# pylint: disable=redefined-outer-name

import pandas as pd
from dagster import (
    AssetOut,
    Config,
    FilesystemIOManager,
    StaticPartitionsDefinition,
    asset,
    get_dagster_logger,
    multi_asset,
)
from sklearn.model_selection import train_test_split

logger = get_dagster_logger()


class RawDataConfig(Config):
    url: str = (
        "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
    )


by_gender = StaticPartitionsDefinition(["male", "female"])


@asset
def raw_data(config: RawDataConfig) -> pd.DataFrame:
    """The raw dataset"""

    return pd.read_csv(config.url)


@asset
def cleaned_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """The preprocessed and cleaned dataset"""

    df = raw_data.dropna()
    df["Survived"] = df["Survived"].apply(bool)
    return raw_data


@asset(partitions_def=by_gender, io_manager_def=FilesystemIOManager())
def partitioned_data(cleaned_data: pd.DataFrame):
    return (
        cleaned_data[cleaned_data["Sex"] == "male"],
        cleaned_data[cleaned_data["Sex"] == "female"],
    )


@multi_asset(
    outs={
        "X_train": AssetOut(),
        "y_train": AssetOut(),
        "X_test": AssetOut(),
        "y_test": AssetOut(),
    }
)
def train_test_set(cleaned_data: pd.DataFrame):
    target_column = "Survived"

    logger.info("Performing train/test split")

    (X_train, X_test, y_train, y_test) = train_test_split(
        cleaned_data.drop(target_column, axis=1), cleaned_data[target_column]
    )

    return X_train, y_train, X_test, y_test
