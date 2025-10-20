FROM python:3.11-slim
#Udates for local builds

# Build argument for git branch and build source
ARG GIT_BRANCH=main
ARG BUILD_SOURCE=

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    GIT_BRANCH=${GIT_BRANCH} \
    BUILD_SOURCE=${BUILD_SOURCE}

# Install system dependencies including PostgreSQL client libraries and tini
RUN apt-get update && apt-get install -y \
    curl \
    libpq-dev \
    gcc \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Create app user and directory
RUN useradd --create-home --shell /bin/bash app
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create git metadata for version detection based on build arg
RUN mkdir -p .git && \
    echo "ref: refs/heads/${GIT_BRANCH}" > .git/HEAD

# Copy DLL to a dedicated directory and create entrypoint script
RUN mkdir -p /app/emby-plugin && \
    cp /app/Emby-DLL/NFOGuard.Emby.Plugin.dll /app/emby-plugin/ && \
    echo '#!/bin/bash' > /app/deploy-plugin.sh && \
    echo 'if [ -d "/emby-plugins" ]; then' >> /app/deploy-plugin.sh && \
    echo '  echo "Deploying NFOGuard Emby Plugin to mounted directory: /emby-plugins"' >> /app/deploy-plugin.sh && \
    echo '  cp /app/emby-plugin/NFOGuard.Emby.Plugin.dll /emby-plugins/' >> /app/deploy-plugin.sh && \
    echo '  echo "Plugin deployed successfully!"' >> /app/deploy-plugin.sh && \
    echo 'elif [ -n "$EMBY_PLUGINS_PATH" ] && [ -d "$EMBY_PLUGINS_PATH" ]; then' >> /app/deploy-plugin.sh && \
    echo '  echo "Deploying NFOGuard Emby Plugin to: $EMBY_PLUGINS_PATH"' >> /app/deploy-plugin.sh && \
    echo '  cp /app/emby-plugin/NFOGuard.Emby.Plugin.dll "$EMBY_PLUGINS_PATH/"' >> /app/deploy-plugin.sh && \
    echo '  echo "Plugin deployed successfully!"' >> /app/deploy-plugin.sh && \
    echo 'else' >> /app/deploy-plugin.sh && \
    echo '  echo "No Emby plugins directory found - skipping plugin deployment"' >> /app/deploy-plugin.sh && \
    echo '  echo "To enable plugin deployment, bind mount your Emby plugins directory to /emby-plugins"' >> /app/deploy-plugin.sh && \
    echo 'fi' >> /app/deploy-plugin.sh && \
    echo 'exec python -u main.py' >> /app/deploy-plugin.sh && \
    chmod +x /app/deploy-plugin.sh

# Set ownership
RUN chown -R app:app /app

# Switch to app user
USER app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose port
EXPOSE 8080

# Use tini as init process to handle signals and zombie processes properly
ENTRYPOINT ["tini", "--"]

# Run the application with plugin deployment
CMD ["/app/deploy-plugin.sh"]