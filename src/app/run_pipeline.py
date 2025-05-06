import os
import click
from app.import_to_raw import import_json_to_parquet


def run_raw():
    source_dir = os.path.join(os.path.dirname(__file__), "../../data/source")
    target_dir = os.path.join(os.path.dirname(__file__), "../../data/raw")
    source_dir = os.path.abspath(source_dir)
    target_dir = os.path.abspath(target_dir)
    import_json_to_parquet(source_dir, target_dir)


def run_target():
    from app import convert_to_target

    convert_to_target.main()
    print("Target mapping completed.")


@click.command()
@click.option("--raw", "stage", flag_value="raw", help="Convert source JSON to raw Parquet files.")
@click.option("--target", "stage", flag_value="target", help="Map raw Parquet files to target Parquet files.")
def main(stage):
    """Data conversion pipeline CLI."""
    if stage == "raw":
        run_raw()
    elif stage == "target":
        run_target()
    else:
        print("Please specify --raw or --target.")


if __name__ == "__main__":
    main()
