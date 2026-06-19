# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — Iris ML Prediction Service
# ─────────────────────────────────────────────────────────────────────────────
#
# Multi-stage approach:
#   Stage 1 (builder): Install all Python dependencies into a virtual env
#   Stage 2 (runtime): Copy only the venv + app code → minimal final image
#
# Using python:3.9-slim as the base keeps the image small (~120 MB final)
# while retaining a standard Debian userland for debugging.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Dependency Builder ───────────────────────────────────────────────
FROM python:3.9-slim AS builder

# Metadata labels (OCI Image Spec)
LABEL org.opencontainers.image.title="iris-ml-api" \
      org.opencontainers.image.description="FastAPI Iris prediction microservice" \
      org.opencontainers.image.version="1.0.0"

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Install build dependencies (needed for some scipy/numpy compile paths)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (layer caching: only re-run pip if requirements change)
COPY requirements.txt .

# Install into a virtual env for clean separation from system Python
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade pip && \
    pip install -r requirements.txt


# ── Stage 2: Runtime Image ────────────────────────────────────────────────────
FROM python:3.9-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    # Application configuration (overridden by Docker Compose)
    MODEL_PATH="/app/model.pkl" \
    METADATA_PATH="/app/model_metadata.json" \
    SERVICE_VERSION="1.0.0" \
    API_PORT="8000"

# Create non-root user for security best practices
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --no-create-home appuser

WORKDIR /app

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy application source code
COPY train_model.py    .
COPY api_server.py     .
COPY kafka_client.py   .

# Train the model at image build time so model.pkl is baked into the image.
# In production, you'd mount model.pkl as a volume or pull from a model registry
# (MLflow, SageMaker, Vertex AI) to separate build from training artifacts.
RUN python train_model.py

# Change ownership to non-root user
RUN chown -R appuser:appgroup /app
USER appuser

# Expose the FastAPI service port
EXPOSE 8000

# Health check: Docker Engine polls this every 30s
# If it fails 3 times, the container is marked "unhealthy" and Compose can restart it
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import requests; r = requests.get('http://localhost:8000/health'); exit(0 if r.json()['status'] == 'healthy' else 1)"

# Start the FastAPI server with Uvicorn
# --workers 1: single worker for demo; scale to (2 * CPU_count + 1) in production
CMD ["uvicorn", "api_server:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info", \
     "--access-log"]
