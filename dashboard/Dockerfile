FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    streamlit==1.29.0 \
    pandas==2.1.4 \
    plotly==5.18.0 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]