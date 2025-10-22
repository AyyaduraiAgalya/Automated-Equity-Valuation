from pathlib import Path
from src.data_prep.silver_transformer.transformer_bs import transform_balance_sheet_to_wide
from src.data_prep.silver_transformer.transformer_is import transform_income_statement_to_wide
from src.data_prep.silver_transformer.transformer_cf import transform_cash_flow_to_wide
from src.data_prep.silver_transformer.transformer_shares import transform_shares_to_wide
from src.data_prep.silver_transformer.transformer_metadata import transform_metadata_to_wide
from src.data_prep.gold_builder.builder_per_zip import build_gold_zip

def build_everything_for_zip(yq: str, raw_dir: Path, silver_dir: Path, gold_dir: Path):
    zip_path = raw_dir / f"{yq}.zip"
    # 1) Silver (idempotent writes)
    transform_balance_sheet_to_wide(zip_path, out_path=silver_dir / f"bs/year_quarter={yq}/bs.parquet")
    transform_income_statement_to_wide(zip_path, out_path=silver_dir / f"is/year_quarter={yq}/is.parquet")
    transform_cash_flow_to_wide(zip_path, out_path=silver_dir / f"cf/year_quarter={yq}/cf.parquet")
    transform_shares_to_wide(zip_path, out_path=silver_dir / f"shares/year_quarter={yq}/shares.parquet")
    transform_metadata_to_wide(zip_path, out_path=silver_dir / f"meta/year_quarter={yq}/meta.parquet")
    # 2) Gold-Z
    return build_gold_zip(yq, silver_dir, gold_dir)
