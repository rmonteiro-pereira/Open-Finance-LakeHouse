import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Static export. There is no server and no runtime data fetch: the numbers are
  // materialised into the HTML at build time from a published release. A reader surface
  // with no backend cannot quietly grow a dependency on the cluster.
  output: "export",
  outputFileTracingRoot: __dirname,
  images: { unoptimized: true },
};

export default nextConfig;
