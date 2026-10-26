# yt-dlp downloads the captions; Deno runs YouTube's JavaScript for it; curl_cffi lets it look
# like a browser, which YouTube now wants for captions. ytsearch.py keeps yt-dlp up to date in
# /data/pydeps (ahead of this copy), because YouTube changes often and old versions break.
FROM python:3.13-slim
RUN pip install --no-cache-dir --root-user-action=ignore "yt-dlp[default,curl-cffi]" deno
COPY ytsearch.py /app/ytsearch.py
COPY web /app/web
ENV DATA_DIR=/data
EXPOSE 8002
CMD ["python", "-u", "/app/ytsearch.py"]
