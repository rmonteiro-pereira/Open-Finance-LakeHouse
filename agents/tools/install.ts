/** Install the approved, content-pinned private release; never commit the archives. */
import {copyFile,mkdir} from "node:fs/promises";
import {resolve} from "node:path";
import {createHash} from "node:crypto";
const source=process.argv[2];if(!source)throw Error("Usage: bun tools/install.ts /path/to/approved/packages");
const root=resolve(import.meta.dir,"..");
const release=await Bun.file(resolve(root,"inkwell-packages.json")).json();
await mkdir(resolve(root,"vendor"),{recursive:true});
for(const entry of release.packages){
 if(!/^[a-zA-Z0-9.-]+\.tgz$/.test(entry.archive)||!/^[a-zA-Z0-9.-]+\.tgz$/.test(entry.vendor))throw Error("Invalid private package filename");
 const input=resolve(source,entry.archive);const sha=createHash("sha256").update(new Uint8Array(await Bun.file(input).arrayBuffer())).digest("hex");
 if(sha!==entry.sha256)throw Error(`Private package checksum mismatch: ${entry.name}`);
 await copyFile(input,resolve(root,"vendor",entry.vendor));
}
const install=Bun.spawn(["bun","install"],{cwd:root,stdout:"inherit",stderr:"inherit"});if(await install.exited)process.exit(1);
