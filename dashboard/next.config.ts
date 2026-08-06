import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Standalone server output for a small, self-contained container image.
  // (Vercel ignores this and uses its own build pipeline.)
  output: "standalone",
  outputFileTracingRoot: __dirname,
};

export default nextConfig;
