#!/usr/bin/env python3
"""
Climate-Smart Agriculture — Spark MLlib Machine Learning Pipeline

Trains a Random Forest Regression model to predict crop yield from
satellite NDVI and weather features. Supports:
  - Distributed k-fold cross-validation with hyperparameter tuning
  - Feature importance analysis
  - Predictions written to PostgreSQL + MongoDB

Usage:
  # With distributed cross-validation (default)
  spark-submit --packages org.postgresql:postgresql:42.6.0 ml_model.py

  # Skip cross-validation (faster, uses simple train/test split)
  spark-submit --packages org.postgresql:postgresql:42.6.0 ml_model.py --no-cv
"""

import os
import sys
import argparse
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pymongo import MongoClient

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

POSTGRES_JDBC = "jdbc:postgresql://postgres:5432/crop_yield_db"
POSTGRES_USER = "postgres"
POSTGRES_PASS = "postgres"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")


# ──────────────────────────────────────────────────────────────────────
# Main Pipeline
# ──────────────────────────────────────────────────────────────────────

def main(use_cross_validation=True):
    print("--- Starting Spark MLlib Machine Learning Pipeline ---")

    # ── Pre-training check for skip logic ──
    postgres_uri = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@postgres:5432/crop_yield_db")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
    current_signature = None

    try:
        import psycopg2
        pg_conn = psycopg2.connect(postgres_uri)
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'yield_features');")
        features_exists = pg_cursor.fetchone()[0]

        pg_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'crop_yields');")
        yields_exists = pg_cursor.fetchone()[0]

        pg_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'yield_predictions');")
        preds_exists = pg_cursor.fetchone()[0]

        if features_exists and yields_exists and preds_exists:
            pg_cursor.execute("SELECT COUNT(*), COALESCE(SUM(avg_ndvi), 0) FROM yield_features;")
            feat_count, feat_sum = pg_cursor.fetchone()

            pg_cursor.execute("SELECT COUNT(*), COALESCE(SUM(yield_mt_ha), 0) FROM crop_yields;")
            yield_count, yield_sum = pg_cursor.fetchone()

            pg_cursor.execute("SELECT COUNT(*) FROM yield_predictions;")
            pred_count = pg_cursor.fetchone()[0]

            pg_cursor.close()
            pg_conn.close()

            current_signature = f"{feat_count}_{float(feat_sum):.4f}_{yield_count}_{float(yield_sum):.4f}"

            # Check MongoDB
            mongo_client = MongoClient(mongo_uri)
            mongo_db = mongo_client["crop_dashboard"]
            saved_meta = mongo_db["model_metadata"].find_one({"type": "random_forest"})
            cv_count = mongo_db["cv_results"].count_documents({})
            feat_imp_count = mongo_db["feature_importance"].count_documents({})

            if (saved_meta and 
                saved_meta.get("signature") == current_signature and 
                pred_count > 0 and 
                cv_count > 0 and 
                feat_imp_count > 0):
                print(f"✓ Model inputs have not changed (signature: {current_signature}) and predictions exist.")
                print("Skipping Random Forest training and cross-validation.")
                return
    except Exception as e:
        print(f"Pre-training check bypassed (tables may not exist yet): {e}")

    spark = SparkSession.builder \
        .appName("ClimateSmartAgriculture-ML") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/java.net=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                "--add-opens=java.security.jgss/sun.security.jgss=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/java.net=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                "--add-opens=java.security.jgss/sun.security.jgss=ALL-UNNAMED") \
        .getOrCreate()

    # ── 1. Load Data from PostgreSQL ──
    print("Loading datasets from PostgreSQL...")
    features_df = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC) \
        .option("dbtable", "yield_features") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    yields_df = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC) \
        .option("dbtable", "crop_yields") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # ── 2. Aggregate 16-day features → annual predictors ──
    print("Aggregating 16-day features to annual predictors...")
    features_with_year = features_df \
        .withColumn("year", F.year(F.to_date(F.col("date"), "yyyy-MM-dd")))

    yearly_features = features_with_year.groupBy("county_id", "year") \
        .agg(
            F.round(F.mean("avg_ndvi"), 4).alias("mean_ndvi"),
            F.round(F.max("avg_ndvi"), 4).alias("max_ndvi"),
            F.round(F.sum("total_precip"), 2).alias("annual_precip"),
            F.round(F.mean("avg_temp"), 2).alias("mean_temp"),
            F.round(F.sum("gdd"), 2).alias("annual_gdd"),
        )

    print("Yearly aggregated feature sample:")
    yearly_features.show(5)

    # ── 3. Join with historical yields ──
    training_data = yearly_features.join(yields_df, on=["county_id", "year"])
    record_count = training_data.count()
    print(f"Total records available for model training: {record_count}")
    training_data.show(5)

    if record_count == 0:
        print("⚠ No training data available. Exiting ML pipeline.")
        spark.stop()
        return

    # ── 4. Feature assembly ──
    from pyspark.ml.feature import StringIndexer, OneHotEncoder

    print("Encoding crop column as a categorical feature...")
    crop_indexer = StringIndexer(inputCol="crop", outputCol="crop_index")
    crop_indexer_model = crop_indexer.fit(training_data)
    indexed_data = crop_indexer_model.transform(training_data)

    crop_encoder = OneHotEncoder(inputCol="crop_index", outputCol="crop_vec")
    crop_encoder_model = crop_encoder.fit(indexed_data)
    encoded_data = crop_encoder_model.transform(indexed_data)

    numerical_cols = ["mean_ndvi", "max_ndvi", "annual_precip", "mean_temp", "annual_gdd", "area_ha"]
    labels = crop_indexer_model.labels
    # OneHotEncoder drops the last category, so it has len(labels) - 1 elements
    encoded_crop_cols = [f"crop_{label}" for label in labels[:-1]]
    all_feature_names = numerical_cols + encoded_crop_cols

    feature_cols = ["mean_ndvi", "max_ndvi", "annual_precip", "mean_temp", "annual_gdd", "area_ha", "crop_vec"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled_data = assembler.transform(encoded_data)
    ml_dataset = assembled_data.select(
        "features", F.col("yield_mt_ha").alias("label"),
        "county_id", "year", "crop"
    )

    # ── 5. Train/test split ──
    train_set, test_set = ml_dataset.randomSplit([0.8, 0.2], seed=42)

    # ── 6. Model Training ──
    rf = RandomForestRegressor(
        featuresCol="features", labelCol="label",
        numTrees=35, maxDepth=6, seed=42
    )

    if use_cross_validation:
        print("\n=== Distributed Cross-Validation with Hyperparameter Tuning ===")

        # Build parameter grid
        paramGrid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [20, 35, 50]) \
            .addGrid(rf.maxDepth, [4, 6, 8]) \
            .build()

        evaluator_rmse = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )

        # 5-fold cross-validation (distributed across cluster)
        crossval = CrossValidator(
            estimator=rf,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator_rmse,
            numFolds=5,
            seed=42,
        )

        print(f"Parameter grid: {len(paramGrid)} combinations × 5 folds = {len(paramGrid) * 5} models")
        print("Training with distributed cross-validation (this may take a few minutes)...")

        try:
            cv_model = crossval.fit(train_set)
            best_model = cv_model.bestModel

            # Log cross-validation results
            avg_metrics = cv_model.avgMetrics
            print(f"\nCross-validation results ({len(avg_metrics)} parameter combinations):")
            for i, (params, metric) in enumerate(zip(paramGrid, avg_metrics)):
                nt = params[rf.numTrees]
                md = params[rf.maxDepth]
                print(f"  [{i+1}] numTrees={nt}, maxDepth={md} → avg RMSE: {metric:.4f}")

            best_idx = avg_metrics.index(min(avg_metrics))
            best_params = paramGrid[best_idx]
            print(f"\n✓ Best model: numTrees={best_params[rf.numTrees]}, maxDepth={best_params[rf.maxDepth]}")
            print(f"  Best avg RMSE: {min(avg_metrics):.4f}")

            rf_model = best_model

            # Save CV results to MongoDB
            cv_results = []
            for i, (params, metric) in enumerate(zip(paramGrid, avg_metrics)):
                cv_results.append({
                    "numTrees": int(params[rf.numTrees]),
                    "maxDepth": int(params[rf.maxDepth]),
                    "avg_rmse": float(metric),
                    "is_best": (i == best_idx),
                })

            try:
                mongo_client = MongoClient(MONGO_URI)
                mongo_db = mongo_client["crop_dashboard"]
                mongo_db["cv_results"].drop()
                mongo_db["cv_results"].insert_many(cv_results)
                print("Cross-validation results saved to MongoDB.")
            except Exception as e:
                print(f"Failed to save CV results to MongoDB: {e}")
        except Exception as cv_error:
            print(f"\n⚠ Cross-validation failed: {cv_error}")
            print("  This usually happens when there is insufficient historical data (e.g. only 1-2 years of tiles).")
            print("  Falling back to training Random Forest model directly on the training set.")
            rf_model = rf.fit(train_set)

    else:
        print("\n=== Simple Train/Test Split (--no-cv mode) ===")
        print("Training Random Forest Regression model...")
        rf_model = rf.fit(train_set)

    # ── 7. Evaluate ──
    print("\nEvaluating model on test set...")
    predictions = rf_model.transform(test_set)
    predictions.show(5)

    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print(f"Evaluation Results → Root Mean Squared Error (RMSE): {rmse:.4f}")
    print(f"Evaluation Results → R-squared (R²) Score: {r2:.4f}")

    # ── 8. Feature Importance ──
    importances = rf_model.featureImportances.toArray()
    feature_importance_records = []
    print("\nFeature Importance:")
    for name, imp in zip(all_feature_names, importances):
        print(f"  {name}: {imp:.4f}")
        feature_importance_records.append({"feature": name, "importance": float(imp)})

    # Add model metrics to feature importance records
    feature_importance_records.append({
        "feature": "_model_rmse", "importance": float(rmse)
    })
    feature_importance_records.append({
        "feature": "_model_r2", "importance": float(r2)
    })

    # Save to MongoDB
    try:
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client["crop_dashboard"]
        mongo_db["feature_importance"].drop()
        mongo_db["feature_importance"].insert_many(feature_importance_records)
        print("Saved feature importances to MongoDB.")
    except Exception as e:
        print(f"Failed to save feature importances: {e}")

    # ── 9. Generate predictions for full dataset ──
    print("\nGenerating predictions across all records...")
    all_predictions = rf_model.transform(assembled_data)

    final_output = all_predictions.select(
        F.col("county_id").cast("int"),
        "crop",
        F.col("year").cast("int"),
        F.col("yield_mt_ha").cast("double").alias("actual_yield"),
        F.round(F.col("prediction"), 4).cast("double").alias("predicted_yield"),
    )

    # Save to PostgreSQL (using truncate=True to keep foreign keys & constraints)
    final_output.write \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC) \
        .option("dbtable", "yield_predictions") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()
    print("Predictions saved to PostgreSQL yield_predictions table.")

    # Save to MongoDB
    try:
        pd_df = final_output.toPandas()
        pd_df["actual_yield"] = pd_df["actual_yield"].astype(float)
        pd_df["predicted_yield"] = pd_df["predicted_yield"].astype(float)
        records = pd_df.to_dict(orient="records")

        mongo_db["yield_predictions"].drop()
        mongo_db["yield_predictions"].insert_many(records)
        print("Predictions inserted into MongoDB yield_predictions collection.")
    except Exception as e:
        print(f"Failed to save predictions to MongoDB: {e}")

    # Save model signature to MongoDB for skip logic
    if current_signature:
        try:
            mongo_client = MongoClient(MONGO_URI)
            mongo_db = mongo_client["crop_dashboard"]
            mongo_db["model_metadata"].update_one(
                {"type": "random_forest"},
                {"$set": {"signature": current_signature}},
                upsert=True
            )
            print("Model metadata signature saved to MongoDB.")
        except Exception as e:
            print(f"Failed to save model metadata signature: {e}")

    spark.stop()
    print("\n✓ ML Pipeline Finished successfully.")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark MLlib Crop Yield Prediction")
    parser.add_argument("--no-cv", action="store_true",
                        help="Skip cross-validation, use simple train/test split")
    args = parser.parse_args()

    main(use_cross_validation=not args.no_cv)
