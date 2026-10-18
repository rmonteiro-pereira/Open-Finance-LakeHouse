/** Consume approved private packages without committing Inkwell source to OFL. */
import { copyFile, mkdir } from "node:fs/promises";
import { resolve } from "node:path";
const source = process.argv[2];
if (!source) throw new Error("Usage: bun tools/install.ts /path/to/approved/inkwell/packages");
const root = resolve(import.meta.dir, "..");
await mkdir(resolve(root, "vendor"), { recursive: true });
for (const name of ["core", "protocol", "server", "store-postgres"]) {
  const file = `inkwell-${name}-0.1.0.tgz`;
  await copyFile(resolve(source, file), resolve(root, "vendor", file));
}
const install = Bun.spawn(["bun", "install"], { cwd: root, stdout: "inherit", stderr: "inherit" });
if (await install.exited) process.exit(1);
