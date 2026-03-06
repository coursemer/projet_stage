#!/usr/bin/env python3
"""
Tests de connectivité Spark
"""

try:
    from pyspark.sql import SparkSession

    # Créer une session Spark
    spark = SparkSession.builder \
        .appName("Test Connection") \
        .master("local[*]") \
        .getOrCreate()

    print("✓ Spark session créée avec succès!")
    print(f"✓ Spark version: {spark.version}")

    # Test simple
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    
    print("\n✓ Test DataFrame:")
    df.show()
    
    spark.stop()
    print("\n✓ Tous les tests Spark réussis!")

except Exception as e:
    print(f"✗ Erreur Spark: {e}")
