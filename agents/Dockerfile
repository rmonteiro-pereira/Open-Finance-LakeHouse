# Build only in a private environment with approved Inkwell tarballs in vendor/.
FROM docker.io/oven/bun:1.4.0
WORKDIR /app
COPY package.json bun.lock ./
COPY vendor ./vendor
RUN bun install --frozen-lockfile --production
COPY src ./src
USER bun
ENV OFL_AGENT_HOST=0.0.0.0 OFL_AGENT_PORT=8082
EXPOSE 8082
CMD ["bun", "src/server.ts"]
