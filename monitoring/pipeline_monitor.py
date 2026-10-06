import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    print("NovaPay Pipeline Monitoring")
    print("---------------------------")

    project_root = Path(__file__).resolve().parents[1]

    bronze_path = project_root / "data" / "bronze"
    silver_path = project_root / "data" / "silver"
    quarantine_path = project_root / "data" / "quarantine"
    gold_path = project_root / "data" / "gold"
    output_path = project_root / "monitoring" / "pipeline_metrics.json"

    spark = (
        SparkSession.builder
        .appName("NovaPayPipelineMonitoring")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    bronze_count = spark.read.parquet(str(bronze_path)).count()
    silver_count = spark.read.parquet(str(silver_path)).count()
    quarantine_count = spark.read.parquet(str(quarantine_path)).count()

    risk_df = spark.read.parquet(
        str(gold_path / "risk_scored_transactions")
    )

    gold_count = risk_df.count()

    high_risk_count = risk_df.filter(
        risk_df.risk_level == "HIGH"
    ).count()

    medium_risk_count = risk_df.filter(
        risk_df.risk_level == "MEDIUM"
    ).count()

    low_risk_count = risk_df.filter(
        risk_df.risk_level == "LOW"
    ).count()

    metrics = {
        "pipeline_name": "NovaPay Financial Transaction Lakehouse",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "SUCCESS",
        "bronze_records": bronze_count,
        "silver_records": silver_count,
        "quarantined_records": quarantine_count,
        "records_removed_during_quality_processing":
            bronze_count - silver_count - quarantine_count,
        "gold_risk_records": gold_count,
        "high_risk_transactions": high_risk_count,
        "medium_risk_transactions": medium_risk_count,
        "low_risk_transactions": low_risk_count,
        "silver_retention_rate_percent":
            round((silver_count / bronze_count) * 100, 2),
        "quarantine_rate_percent":
            round((quarantine_count / bronze_count) * 100, 2),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(metrics, file, indent=4)

    print()
    print(json.dumps(metrics, indent=4))

    print()
    print(f"Monitoring metrics written to: {output_path}")
    print("Pipeline monitoring completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()