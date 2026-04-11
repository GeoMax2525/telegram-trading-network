FROM python:3.12-slim

# Install Node.js (for gmgn-cli)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install gmgn-cli globally
RUN npm install -g gmgn-cli

# Setup Python app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Configure gmgn-cli from env vars at runtime
CMD bash -c '\
    mkdir -p ~/.config/gmgn && \
    if [ -n "$GMGN_API_KEY" ]; then \
        echo "GMGN_API_KEY=$GMGN_API_KEY" > ~/.config/gmgn/.env; \
        if [ -n "$GMGN_PRIVATE_KEY" ]; then \
            printf "GMGN_PRIVATE_KEY=\"-----BEGIN PRIVATE KEY-----\n%s\n-----END PRIVATE KEY-----\"\n" "$GMGN_PRIVATE_KEY" >> ~/.config/gmgn/.env; \
        fi; \
        chmod 600 ~/.config/gmgn/.env; \
        echo "gmgn-cli configured"; \
    fi && \
    python main.py'
