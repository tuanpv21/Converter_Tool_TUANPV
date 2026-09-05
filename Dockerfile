FROM python:3.10-slim

# Tao user non-root UID 1000 theo chuan bao mat cua Hugging Face Spaces
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=7860

WORKDIR $HOME/app

# Copy va cai dat requirements
COPY --chown=user:user requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Copy toan bo source code
COPY --chown=user:user . .

# Hugging Face Spaces lang nghe o port 7860
EXPOSE 7860

# Khoi chay ung dung web
CMD ["python", "app_web.py"]
