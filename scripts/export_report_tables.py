"""Copy final Spark CSV part files into tracked, single-file report tables."""
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "reports" / "results"
DESTINATION = ROOT / "reports" / "tables"


def copy_part(source_dir: Path, output_name: str) -> None:
    parts = sorted(source_dir.glob("part-*.csv"))
    if not parts:
        raise FileNotFoundError(f"No Spark CSV part file found in {source_dir}")
    DESTINATION.mkdir(parents=True, exist_ok=True)
    target = DESTINATION / output_name
    shutil.copyfile(parts[0], target)
    print(f"Copied {source_dir.name} -> {target.relative_to(ROOT)}")


def main() -> None:
    for result_dir in sorted((RESULTS / "exploration").glob("*.csv")):
        if result_dir.is_dir():
            output_name = f"{result_dir.name[:-4]}.csv"
            copy_part(result_dir, output_name)
    copy_part(RESULTS / "null_rates.csv", "null_rates.csv")
    copy_part(RESULTS / "top_null_columns.csv", "top_null_columns.csv")


if __name__ == "__main__":
    main()
