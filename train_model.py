"""
train_model.py
==============
Trains a LogisticRegression classifier on the Iris dataset and serializes
the fitted model to disk as model.pkl using joblib.

Discovery-to-Action (DTA) Step: Discovery Phase
- This script is the entry point of the ML pipeline.
- Run once to produce model.pkl, which is consumed by api_server.py at startup.
- In production, this script would be triggered by a CI/CD pipeline whenever
  new training data arrives, ensuring the model stays current.
"""

import joblib
import numpy as np
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load Dataset
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  Iris ML Pipeline — Model Training")
print("=" * 60)

iris = load_iris()
X, y = iris.data, iris.target
feature_names = iris.feature_names
class_names = iris.target_names

print(f"\n[1/4] Dataset loaded")
print(f"      Features  : {feature_names}")
print(f"      Classes   : {list(class_names)}")
print(f"      Samples   : {X.shape[0]} rows × {X.shape[1]} features")

# ──────────────────────────────────────────────────────────────────────────────
# 2. Train/Test Split
# ──────────────────────────────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"\n[2/4] Train/test split complete")
print(f"      Training samples : {len(X_train)}")
print(f"      Test samples     : {len(X_test)}")

# ──────────────────────────────────────────────────────────────────────────────
# 3. Build Pipeline: StandardScaler + LogisticRegression
# ──────────────────────────────────────────────────────────────────────────────
# Wrapping in a sklearn Pipeline means the scaler is serialized alongside
# the model — the API server applies identical preprocessing at inference time.
model_pipeline = Pipeline([
    ("scaler", StandardScaler()),
    ("classifier", LogisticRegression(
        max_iter=1000,
        random_state=42,
        solver="lbfgs"
    ))
])

model_pipeline.fit(X_train, y_train)
print(f"\n[3/4] Model trained  (LogisticRegression + StandardScaler pipeline)")

# ──────────────────────────────────────────────────────────────────────────────
# 4. Evaluate & Serialize
# ──────────────────────────────────────────────────────────────────────────────
y_pred = model_pipeline.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)

print(f"\n[4/4] Evaluation results")
print(f"      Accuracy : {accuracy * 100:.2f}%")
print("\n      Classification Report:")
print(classification_report(y_test, y_pred, target_names=class_names))

# Serialize the fitted pipeline (includes scaler) to disk
MODEL_PATH = "model.pkl"
joblib.dump(model_pipeline, MODEL_PATH)
print(f"      Model serialized → {MODEL_PATH}")

# Also persist metadata for the API server to expose via /info endpoint
import json

metadata = {
    "model_type": "LogisticRegression",
    "preprocessing": "StandardScaler",
    "dataset": "Iris",
    "features": list(feature_names),
    "classes": list(class_names),
    "accuracy": round(accuracy, 4),
    "training_samples": len(X_train),
    "test_samples": len(X_test)
}

with open("model_metadata.json", "w") as f:
    json.dump(metadata, f, indent=2)

print(f"      Metadata saved  → model_metadata.json")
print("\n" + "=" * 60)
print("  Training complete. Run docker-compose up to launch the stack.")
print("=" * 60)
