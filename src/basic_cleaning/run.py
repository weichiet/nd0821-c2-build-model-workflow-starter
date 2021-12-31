#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import os
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact.
    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()
    df = pd.read_csv(artifact_path)

    # Drop outliers
    logger.info("Dropping outliers")
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting last_review to datetime type")
    df['last_review'] = pd.to_datetime(df['last_review'])

    #Save new df to CSV without the index column
    logger.info("Saving new CSV")
    filename = "clean_sample.csv"
    df.to_csv(filename, index=False)

    #Upload the file to W&B
    logger.info("Creating artifact")
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)

    logger.info("Logging artifact")
    run.log_artifact(artifact)
    os.remove(filename)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact",
        type = str,
        help = "Fully-qualified name for the input artifact",
        required = True
    )

    parser.add_argument(
        "--output_artifact",
        type = str,
        help = "Name for the artifact",
        required = True
    )

    parser.add_argument(
        "--output_type",
        type = str,
        help = "Type for the artifact",
        required = True
    )

    parser.add_argument(
        "--output_description",
        type = str,
        help = "Description for the artifact",
        required = True
    )

    parser.add_argument(
        "--min_price",
        type = float,
        help = "Minimum price per night",
        required = True
    )

    parser.add_argument(
        "--max_price",
        type = float,
        help = "Maximun price per night",
        required = True
    )


    args = parser.parse_args()

    go(args)
