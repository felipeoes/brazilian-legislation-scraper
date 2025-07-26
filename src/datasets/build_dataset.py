import re
import os
import warnings
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, concat, when
from pyspark.sql.types import StringType
from dotenv import load_dotenv
from markdownify import markdownify as md
from urllib.parse import unquote
from pathlib import Path
from datasets import Dataset
from tqdm import tqdm

sys.setrecursionlimit(5000)

# remove pd warning
warnings.simplefilter(action="ignore", category=FutureWarning)

load_dotenv()

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN")
ONEDRIVE_SAVE_DIR = os.getenv("ONEDRIVE_SAVE_DIR")
DIR_PATH = Path(ONEDRIVE_SAVE_DIR)


class DatasetBuilder:
    """Dataset builder from scrapped data in json format."""

    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("DatasetBuilder")
            .master("local[16]")
            .getOrCreate()
        )

    def _load_data(self, base_dir_path: Path) -> DataFrame:
        """Load data from json files"""
        json_files = list(tqdm(
            base_dir_path.glob("**/*.json"),
            desc="Loading JSON files",
            unit="file",
        ))
        if not json_files:
            raise FileNotFoundError(f"No JSON files found in {base_dir_path}")
        
        df = self.spark.read.json([str(file) for file in json_files], multiLine=True)
        return df

    def build_dataset(
        self,
        base_dir_path: Path,
        dataset_name: str,
        output_path: Path,
    ):
        """Build dataset from json files"""
        df = self._load_data(base_dir_path)
        
        print(f"Loaded {df.count()} rows from {len(df.columns)} columns")

        # Drop duplicates based on 'document_url' column
        df = df.dropDuplicates(["document_url"])

        print(f"Dataset shape: ({df.count()}, {len(df.columns)})")
        print(f"Dataset columns: {df.columns}")

        # join 'html_string' and 'pdf_content' columns if both exists
        if "html_string" in df.columns and "pdf_content" in df.columns:
            df = df.withColumn(
                "text",
                concat(
                    when(col("html_string").isNotNull(), col("html_string")).otherwise(""),
                    when(col("pdf_content").isNotNull(), col("pdf_content")).otherwise(""),
                ),
            ).drop("html_string", "pdf_content")
        elif "html_string" in df.columns:
            df = df.withColumnRenamed("html_string", "text")
        elif "pdf_content" in df.columns:
            df = df.withColumnRenamed("pdf_content", "text")

        # convert html or pdf to markdown. Remove img and a tags. Regex replaces four or more '\n' with three '\n'
        def clean_text(text):
            if not text:
                return ""
            regex = re.compile(r"\n{4,}")
            markdown_text = md(str(text), heading_style="ATX", strip=["img", "a"])
            return regex.sub("\n\n\n", markdown_text).strip()

        clean_text_udf = udf(clean_text, StringType())
        df = df.withColumn("text", clean_text_udf(col("text")))

        # sanitize columns
        cols = ["type", "situation", "summary"]
        for c in cols:
            if c in df.columns:
                unquote_udf = udf(lambda x: unquote(x) if x else x, StringType())
                df = df.withColumn(c, unquote_udf(col(c)))

        if "year" in df.columns:
            df = df.withColumn("year", col("year").cast("long"))

        # save without index
        # Coalesce to a single partition to write a single CSV file
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_path))


        # print first ten rows
        df.show(10)

        # save to huggingface datasets
        # Converting to pandas dataframe to push to hub
        pandas_df = df.toPandas()
        dataset = Dataset.from_pandas(pandas_df)
        dataset.push_to_hub(dataset_name, token=HUGGINGFACE_TOKEN, private=True)

    def stop(self):
        self.spark.stop()


if __name__ == "__main__":
    builder = None
    try:
        output_dir = Path(__file__).resolve().parents[2] / "csv-datasets"
        output_dir.mkdir(parents=True, exist_ok=True)

        builder = DatasetBuilder()

        # Legislacao federal
        federal_dir_path = DIR_PATH / "LEGISLACAO_FEDERAL"
        federal_output_path = output_dir / "federal_dataset.csv"
        federal_dataset_name = "felipeoes/br_federal_legislation_v3"
        print("Building federal legislation dataset...")
        builder.build_dataset(
            federal_dir_path, federal_dataset_name, federal_output_path
        )

        # Legislacao especifica
        especifica_dir_path = DIR_PATH / "LEGISLACAO_ESPECIFICA"
        especifica_output_path = output_dir / "especifica_dataset.csv"
        especifica_dataset_name = "felipeoes/br_environment_legislation"
        print("Building environment legislation dataset...")
        builder.build_dataset(
            especifica_dir_path, especifica_dataset_name, especifica_output_path
        )

        # Legislacao estadual
        estadual_dir_path = DIR_PATH / "LEGISLACAO_ESTADUAL"
        estadual_output_path = output_dir / "estadual_dataset.csv"
        estadual_dataset_name = "felipeoes/br_state_legislation"
        print("Building state legislation dataset...")
        builder.build_dataset(
            estadual_dir_path, estadual_dataset_name, estadual_output_path
        )

    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        if builder:
            builder.stop()