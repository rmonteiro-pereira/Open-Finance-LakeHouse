# syntax=docker/dockerfile:1
# ---- Ledger dashboard (Next.js standalone) -------------------------------- #
# Node-only image. Data lives in /app/public/data as JSON; baked synthetic
# snapshot is the fallback. On the cluster, the `ofl-dash-refresh` CronJob
# overwrites that directory (via a shared PVC) with a live Delta snapshot.

FROM node:22-alpine AS deps
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml* ./
# --ignore-scripts: skip optional native build scripts (sharp / unrs-resolver).
# next/image isn't used here, and ESLint is disabled during the build, so neither
# is needed for `next build`. Avoids pnpm 10's ERR_PNPM_IGNORED_BUILDS hard-fail.
RUN pnpm install --frozen-lockfile --ignore-scripts

FROM node:22-alpine AS builder
WORKDIR /app
RUN corepack enable
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# Ensure a snapshot exists at build (synthetic fallback baked into the image).
RUN node --version && pnpm build

FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
RUN addgroup -g 1001 -S nodejs && adduser -S nextjs -u 1001

COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs
EXPOSE 3000
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
CMD ["node", "server.js"]
