import re
import os
import warnings
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, lower, trim, input_file_name
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from dotenv import load_dotenv
from urllib.parse import unquote
from pathlib import Path
from datasets import Dataset, DatasetDict
from tqdm import tqdm

# Set environment variables for Spark to use the same Python environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# remove pd warning
warnings.simplefilter(action="ignore", category=FutureWarning)

load_dotenv()

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN")
DIR_PATH = Path("/mnt/c/Users/Docker/SynologyDrive")


class DatasetBuilder:
    """Dataset builder from scrapped data in json format."""

    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("DatasetBuilder")
            .master("local[32]")  # Reduce parallelism to use less memory
            .config("spark.driver.memory", "32g")  # Increase driver memory
            .config("spark.driver.maxResultSize", "8g")  # Increase max result size
            .config(
                "spark.sql.execution.arrow.pyspark.enabled", "true"
            )  # Enable Arrow for better performance
            .config(
                "spark.sql.adaptive.enabled", "true"
            )  # Enable adaptive query execution
            .config(
                "spark.sql.adaptive.coalescePartitions.enabled", "true"
            )  # Enable partition coalescing
            .config("spark.sql.adaptive.skewJoin.enabled", "true")  # Handle data skew
            .config(
                "spark.sql.execution.arrow.maxRecordsPerBatch", "10000"
            )  # Reduce batch size
            .config(
                "spark.sql.execution.pythonUDF.arrow.enabled", "true"
            )  # Enable Arrow for UDFs
            .getOrCreate()
        )

    def _load_data(self, base_dir_path: Path) -> DataFrame:
        """Load data from json files"""
        json_files = list(
            tqdm(
                base_dir_path.glob("**/*.json"),
                desc="Loading JSON files",
                unit="file",
            )
        )
        if not json_files:
            raise FileNotFoundError(f"No JSON files found in {base_dir_path}")

        schema = StructType(
            [
                StructField("year", IntegerType(), True),
                StructField("title", StringType(), True),
                StructField("type", StringType(), True),
                StructField("situation", StringType(), True),
                StructField("summary", StringType(), True),
                StructField("text_markdown", StringType(), True),
                StructField("document_url", StringType(), True),
            ]
        )
        df = self.spark.read.json(
            [str(file) for file in json_files], multiLine=True, schema=schema
        )
        df = df.withColumn("input_path", input_file_name())
        return df

    def build_dataset(
        self,
        base_dir_path: Path,
        dataset_name: str,
        valid_situations: list[str],
    ):
        """Build dataset from json files"""
        df = self._load_data(base_dir_path)

        print(f"Loaded {df.count()} rows from {len(df.columns)} columns")

        # Repartition the DataFrame to optimize processing and reduce memory usage
        df = df.repartition(400)

        # Drop duplicates based on 'text_markdown', and 'document_url' columns
        # Use a more memory-efficient approach by processing in smaller chunks
        df = df.dropDuplicates(["text_markdown", "document_url"])

        print(f"Dataset shape after deduplication: ({df.count()}, {len(df.columns)})")
        print(f"Dataset columns: {df.columns}")

        # clean text_markdown column and ensure it's utf-8 encoded
        def clean_text(text: str) -> str:
            if not text:
                return text

            text = unquote(text)
            # replace more than two new lines with two new lines
            text = re.sub(r"\n{3,}", "\n\n", text)
            # replace multiple spaces with a single space
            text = re.sub(r" {2,}", " ", text)
            text = text.strip()
            return text.encode("utf-8", "ignore").decode("utf-8")

        clean_text_udf = udf(clean_text, StringType())
        df = df.withColumn("text_markdown", clean_text_udf(col("text_markdown")))

        # remove rows with empty 'title', 'text_markdown' or 'document_url'
        empty_rows = df.filter(
            col("title").isNull()
            | (col("title") == "")
            | (col("text_markdown").isNull() | (col("text_markdown") == ""))
            | (col("document_url").isNull() | (col("document_url") == ""))
        )
        if empty_rows.count() > 0:
            print(
                f"Removing {empty_rows.count()} rows with empty 'text_markdown' or 'document_url'"
            )

            df = df.filter(
                ~(
                    (col("text_markdown").isNull() | (col("text_markdown") == ""))
                    | (col("document_url").isNull() | (col("document_url") == ""))
                )
            )

        # remove rows with with html tag in text_markdown
        html_rows = df.filter(
            col("text_markdown").rlike(r"(?i)<\/?html>|<!DOCTYPE html>")
        )
        if html_rows.count() > 0:
            print(
                f"Removing {html_rows.count()} rows with HTML tags in 'text_markdown'"
            )
            df = df.filter(
                ~col("text_markdown").rlike(r"(?i)<\/?html>|<!DOCTYPE html>")
            )

        # sanitize columns
        cols = ["title", "type", "situation", "summary"]
        for c in cols:
            if c in df.columns:
                unquote_udf = udf(lambda x: unquote(x) if x else x, StringType())
                df = df.withColumn(c, unquote_udf(col(c)))

        # Clean situation column
        if "situation" in df.columns:
            df = df.withColumn("situation_cleaned", lower(trim(col("situation"))))

        # Clean situations lists
        cleaned_valid_situations = [
            unquote(s).lower().strip() for s in valid_situations
        ]

        cols_to_keep = [
            "year",
            "title",
            "type",
            "situation",
            "summary",
            "text_markdown",
            "document_url",
        ]
        # padronize cols to keep only the necessary ones
        if "br_state_legislation" in dataset_name:

            def get_uf(path: str) -> str:
                return path.split("/")[-3]

            get_uf_udf = udf(get_uf, StringType())
            df = df.withColumn("uf", get_uf_udf(col("input_path")))
            cols_to_keep.append("uf")

        # Filter dataframes
        valid_df = df.filter(col("situation_cleaned").isin(cleaned_valid_situations))
        invalid_df = df.filter(~col("situation_cleaned").isin(cleaned_valid_situations))

        valid_df = valid_df.select(*cols_to_keep)
        invalid_df = invalid_df.select(*cols_to_keep)

        # valid_df.show(10, truncate=False)
        # invalid_df.show(10, truncate=False)

        valid_dataset = []
        invalid_dataset = []
        if valid_df.count() > 0:
            valid_dataset = Dataset.from_spark(valid_df)
            print(f"Valid dataset size: {valid_dataset.num_rows}")

        if not valid_dataset:
            print("No valid dataset found. Exiting...")
            return

        if invalid_df.count() > 0:
            invalid_dataset = Dataset.from_spark(invalid_df)
            print(f"Invalid dataset size: {invalid_dataset.num_rows}")

        dataset = DatasetDict({"valid": valid_dataset})

        # add invalid dataset if it exists
        if invalid_dataset:
            dataset["invalid"] = invalid_dataset

        print("Pushing to Hugging Face Hub...")
        dataset.push_to_hub(dataset_name, token=HUGGINGFACE_TOKEN)

    def stop(self):
        self.spark.stop()


if __name__ == "__main__":
    builder = None
    try:
        builder = DatasetBuilder()

        # Set Spark log level to reduce output noise
        builder.spark.sparkContext.setLogLevel("WARN")

        valid_situations = [
            "Não consta",
            "Não consta revogação expressa",
            "Não%20consta%20revogação%20expressa",
            "Não Informado",
            "Convertida%20em%20Lei",
            "Reeditada",
            "Reeditada%20com%20alteração",
            "Em Vigor",
            "Sem revogação expressa",
            "Sem Revogação Expressa",
            "Ajuizado",
            "Alterado",
            "Julgado Procedente",
            "Não conhecida",
        ]

        # Legislacao federal
        # federal_dir_path = DIR_PATH / "LEGISLACAO_FEDERAL"
        # federal_dataset_name = "felipeoes/br_federal_legislation_v3"
        # print("Building federal legislation dataset...")
        # builder.build_dataset(federal_dir_path, federal_dataset_name, valid_situations)

        # Legislacao especifica
        especifica_dir_path = DIR_PATH / "LEGISLACAO_ESPECIFICA"
        especifica_dataset_name = "felipeoes/br_environment_legislation"
        print("Building environment legislation dataset...")
        builder.build_dataset(
            especifica_dir_path, especifica_dataset_name, valid_situations
        )

        # # Legislacao estadual
        # estadual_dir_path = DIR_PATH / "LEGISLACAO_ESTADUAL"
        # estadual_dataset_name = "felipeoes/br_state_legislation"
        # print("Building state legislation dataset...")
        # builder.build_dataset(
        #     estadual_dir_path, estadual_dataset_name, valid_situations
        # )

    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        if builder:
            builder.stop()
