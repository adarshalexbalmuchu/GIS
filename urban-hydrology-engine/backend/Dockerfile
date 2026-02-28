FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev gcc g++ \
    libgdal-dev gdal-bin \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY requirements.txt .
# Install GDAL pip package matching the system libgdal version
RUN pip install --no-cache-dir numpy==1.26.4 \
    && pip install --no-cache-dir GDAL==$(gdal-config --version) \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
