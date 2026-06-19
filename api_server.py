"""
api_server.py
=============
FastAPI service that loads the serialized Scikit-Learn pipeline at startup
and exposes a /predict POST endpoint for real-time Iris classification.

Discovery-to-Action (DTA) Step: Technical Phase
- Receives JSON feature arrays from HTTP clients OR from the Kafka producer.
- Runs model inference and returns structured prediction responses.
- Designed for containerized deployment: reads MODEL_PATH and KAFKA_BROKER
  from environment variables with sensible defaults for local dev.

Architecture note:
  HTTP Client → POST /predict → FastAPI → model.pkl → JSON response
  Kafka         → ml-requests topic → kafka_client consumer → POST /predict
"""

import os
import json
import time
import uuid
import logging
from contextlib import asynccontextmanager
from typing import List

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("api_server")

# ──────────────────────────────────────────────────────────────────────────────
# Config from environment (Docker Compose injects these)
# ──────────────────────────────────────────────────────────────────────────────
MODEL_PATH = os.getenv("MODEL_PATH", "model.pkl")
METADATA_PATH = os.getenv("METADATA_PATH", "model_metadata.json")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")

# Iris class labels (fallback if metadata missing)
CLASS_NAMES = ["setosa", "versicolor", "virginica"]
FEATURE_NAMES = [
    "sepal length (cm)",
    "sepal width (cm)",
    "petal length (cm)",
    "petal width (cm)"
]

# ──────────────────────────────────────────────────────────────────────────────
# Global model state (loaded once at startup)
# ──────────────────────────────────────────────────────────────────────────────
model_pipeline = None
model_metadata: dict = {}

# ──────────────────────────────────────────────────────────────────────────────
# Lifespan: load model before accepting requests
# ──────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model and metadata on startup; clean up on shutdown."""
    global model_pipeline, model_metadata, CLASS_NAMES, FEATURE_NAMES

    logger.info("Starting ML Prediction Service v%s", SERVICE_VERSION)

    # Load serialized model pipeline
    if not os.path.exists(MODEL_PATH):
        logger.error("model.pkl not found at %s — run train_model.py first", MODEL_PATH)
        raise FileNotFoundError(f"Model file not found: {MODEL_PATH}")

    model_pipeline = joblib.load(MODEL_PATH)
    logger.info("Model loaded from %s", MODEL_PATH)

    # Load optional metadata
    if os.path.exists(METADATA_PATH):
        with open(METADATA_PATH) as f:
            model_metadata = json.load(f)
        CLASS_NAMES = model_metadata.get("classes", CLASS_NAMES)
        FEATURE_NAMES = model_metadata.get("features", FEATURE_NAMES)
        logger.info(
            "Metadata loaded — model: %s, accuracy: %.2f%%",
            model_metadata.get("model_type"),
            model_metadata.get("accuracy", 0) * 100
        )

    yield  # Application runs here

    logger.info("Shutting down ML Prediction Service")


# ──────────────────────────────────────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Iris ML Prediction Service",
    description=(
        "Production-grade FastAPI microservice for real-time Iris species "
        "classification. Integrates with Kafka for asynchronous streaming."
    ),
    version=SERVICE_VERSION,
    lifespan=lifespan
)


# ──────────────────────────────────────────────────────────────────────────────
# Request / Response Schemas (Pydantic v2 compatible)
# ──────────────────────────────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    """
    Input schema for a single prediction request.

    features: 4-element list [sepal_length, sepal_width, petal_length, petal_width]
    request_id: optional idempotency key; auto-generated if omitted
    """
    features: List[float] = Field(
        ...,
        example=[5.1, 3.5, 1.4, 0.2],
        description="[sepal_length, sepal_width, petal_length, petal_width] in cm"
    )
    request_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique request identifier for tracing"
    )

    @validator("features")
    def validate_features(cls, v):
        if len(v) != 4:
            raise ValueError(
                f"Expected 4 features, got {len(v)}. "
                "Order: [sepal_length, sepal_width, petal_length, petal_width]"
            )
        if any(val < 0 for val in v):
            raise ValueError("Feature values must be non-negative measurements (cm)")
        return v


class PredictResponse(BaseModel):
    """Structured prediction response with confidence scores."""
    request_id: str
    prediction_label: str
    prediction_index: int
    confidence: float
    probabilities: dict
    latency_ms: float
    model_version: str


class BatchPredictRequest(BaseModel):
    """Batch inference: up to 100 samples in a single call."""
    samples: List[List[float]] = Field(..., max_items=100)

    @validator("samples")
    def validate_samples(cls, v):
        for i, sample in enumerate(v):
            if len(sample) != 4:
                raise ValueError(f"Sample {i}: expected 4 features, got {len(sample)}")
        return v


# ──────────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
async def root():
    """Service root — confirms the API is alive."""
    return {
        "service": "Iris ML Prediction Service",
        "version": SERVICE_VERSION,
        "status": "running",
        "endpoints": {
            "single_predict": "POST /predict",
            "batch_predict": "POST /predict/batch",
            "model_info": "GET /info",
            "health": "GET /health"
        }
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint used by Docker Compose and load balancers."""
    model_ready = model_pipeline is not None
    return {
        "status": "healthy" if model_ready else "unhealthy",
        "model_loaded": model_ready,
        "model_path": MODEL_PATH
    }


@app.get("/info", tags=["Model"])
async def model_info():
    """Returns model metadata: type, features, class labels, training accuracy."""
    if not model_metadata:
        return {"message": "No metadata file found. Run train_model.py to generate."}
    return model_metadata


@app.post("/predict", response_model=PredictResponse, tags=["Prediction"])
async def predict(request: PredictRequest):
    """
    Single-sample prediction endpoint.

    Accepts a JSON body with 4 Iris feature measurements and returns:
    - Predicted class label (setosa / versicolor / virginica)
    - Confidence score (max class probability)
    - Full probability distribution across all 3 classes
    - Request-to-response latency in milliseconds
    """
    if model_pipeline is None:
        raise HTTPException(status_code=503, detail="Model not loaded. Service unavailable.")

    t_start = time.perf_counter()

    try:
        # Reshape for sklearn: (1, 4) array
        X = np.array(request.features).reshape(1, -1)

        # Run inference (scaler + classifier in pipeline)
        pred_index = int(model_pipeline.predict(X)[0])
        pred_probs = model_pipeline.predict_proba(X)[0]

        # Build structured response
        pred_label = CLASS_NAMES[pred_index]
        confidence = float(round(pred_probs[pred_index], 4))
        probabilities = {
            CLASS_NAMES[i]: float(round(p, 4))
            for i, p in enumerate(pred_probs)
        }

    except Exception as e:
        logger.exception("Inference error for request %s", request.request_id)
        raise HTTPException(status_code=500, detail=f"Inference failed: {str(e)}")

    latency_ms = round((time.perf_counter() - t_start) * 1000, 3)

    logger.info(
        "request_id=%s | features=%s | prediction=%s | confidence=%.2f%% | latency=%.2fms",
        request.request_id, request.features, pred_label, confidence * 100, latency_ms
    )

    return PredictResponse(
        request_id=request.request_id,
        prediction_label=pred_label,
        prediction_index=pred_index,
        confidence=confidence,
        probabilities=probabilities,
        latency_ms=latency_ms,
        model_version=SERVICE_VERSION
    )


@app.post("/predict/batch", tags=["Prediction"])
async def batch_predict(request: BatchPredictRequest):
    """
    Batch prediction endpoint for up to 100 samples.

    Efficient for bulk inference without Kafka overhead — ideal for
    backfill jobs or scheduled scoring runs.
    """
    if model_pipeline is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")

    t_start = time.perf_counter()

    X = np.array(request.samples)
    pred_indices = model_pipeline.predict(X).tolist()
    pred_probs = model_pipeline.predict_proba(X)

    results = []
    for i, (idx, probs) in enumerate(zip(pred_indices, pred_probs)):
        results.append({
            "sample_index": i,
            "prediction_label": CLASS_NAMES[idx],
            "prediction_index": idx,
            "confidence": float(round(probs[idx], 4)),
            "probabilities": {
                CLASS_NAMES[j]: float(round(p, 4)) for j, p in enumerate(probs)
            }
        })

    total_latency_ms = round((time.perf_counter() - t_start) * 1000, 3)

    return {
        "batch_size": len(request.samples),
        "results": results,
        "total_latency_ms": total_latency_ms,
        "avg_latency_ms": round(total_latency_ms / len(request.samples), 3)
    }


# ──────────────────────────────────────────────────────────────────────────────
# Entry point (for local development without Docker)
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
