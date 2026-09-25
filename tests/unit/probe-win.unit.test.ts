import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { registerS3ObjectTools } from "../../src/s3/objects";
import type { B2S3VersionGuard } from "../../src/utils/types";
import { DeterministicS3ClientFake, ToolHarness, testConfig } from "../support/deterministic-fakes";

// Fork-only probe: why does "leaves an empty same-named directory" fail on Windows?
describe("probe: Windows directory identity and rename semantics", () => {
  it("reports what the platform does", async () => {
    const out: Record<string, unknown> = { platform: process.platform, node: process.version };
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "probe-win-"));
    try {
      // A: rename a directory that holds an open file.
      const a = path.join(root, "a");
      fs.mkdirSync(a);
      const h = await fs.promises.open(path.join(a, "f.part"), "wx");
      try {
        fs.renameSync(a, `${a}.moved`);
        out.renameDirWithOpenFile = "ok";
      } catch (err) {
        out.renameDirWithOpenFile = (err as NodeJS.ErrnoException).code;
      }
      await h.close();

      // B: identity of a directory and an empty same-named replacement.
      const x = path.join(root, "x");
      fs.mkdirSync(x);
      const first = fs.lstatSync(x);
      const firstBig = fs.lstatSync(x, { bigint: true });
      fs.renameSync(x, `${x}.moved`);
      fs.mkdirSync(x);
      const second = fs.lstatSync(x);
      const secondBig = fs.lstatSync(x, { bigint: true });
      out.ino = { first: first.ino, second: second.ino, equal: first.ino === second.ino && first.dev === second.dev };
      out.inoBig = { first: String(firstBig.ino), second: String(secondBig.ino), equal: firstBig.ino === secondBig.ino };
      out.inoExceeds2e53 = firstBig.ino > BigInt(Number.MAX_SAFE_INTEGER);

      // C: the failing fixture scenario, recording what happened at each step.
      const s3 = new DeterministicS3ClientFake();
      const versionGuard = {
        async resolveS3FileVersion() { throw new Error("unused"); },
        async resolveS3FileVersions(input: { objects: Array<{ key: string }> }) {
          return input.objects.map((object) => ({ object, version: null }));
        },
        async getCurrentS3FileVersion() { return null; },
      } as unknown as B2S3VersionGuard;
      const tools = new ToolHarness();
      registerS3ObjectTools(tools, s3.asPeerClient(), versionGuard, testConfig);
      const shared = path.join(root, "shared");
      fs.mkdirSync(shared);
      const level = path.join(shared, "x");
      s3.respond("getObject", () => {
        try {
          fs.renameSync(level, `${level}.moved`);
          out.scenarioRename = "ok";
        } catch (err) {
          out.scenarioRename = (err as NodeJS.ErrnoException).code;
        }
        try {
          fs.mkdirSync(level);
          out.scenarioDecoy = "ok";
        } catch (err) {
          out.scenarioDecoy = (err as NodeJS.ErrnoException).code;
        }
        throw Object.assign(new Error("NoSuchKey"), { name: "NoSuchKey", $metadata: { httpStatusCode: 404 } });
      });
      const result = await tools.call("s3_get_object", { bucket: "b", key: "missing.txt", saveToPath: path.join(level, "out.txt") });
      out.scenarioIsError = Boolean(result.isError);
      out.scenarioLevelExists = fs.existsSync(level);
      out.scenarioMovedExists = fs.existsSync(`${level}.moved`);
      out.scenarioMovedListing = fs.existsSync(`${level}.moved`) ? fs.readdirSync(`${level}.moved`) : null;
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
    expect(JSON.stringify(out)).toBe("probe");
  });
});
