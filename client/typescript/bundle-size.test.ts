import { expect, it } from "bun:test";
import { brotliCompressSync } from "node:zlib";

it("uncompressed bundle size must not exceed 40kb", async () => {
	await Bun.build({
		entrypoints: ["./client/typescript/index.ts"],
		outdir: "./dist/uncompressed",
		minify: true,
		format: "esm",
	});
	const bundle = Bun.file("dist/uncompressed/index.js");
	console.log(`Uncompressed bundle size is ${bundle.size / 1000}kb`);
	expect(bundle.size).toBeLessThan(40_000);
});

it("compressed bundle size must not exceed 10kb", async () => {
	await Bun.build({
		entrypoints: ["./client/typescript/index.ts"],
		outdir: "./dist/compressed",
		minify: true,
		format: "esm",
	});
	const bundle = Bun.file("dist/compressed/index.js");
	const bytes = await bundle.bytes();
	const compressed = brotliCompressSync(bytes);
	console.log(`Compressed bundle size is ${compressed.length / 1000}kb`);
	expect(compressed.length).toBeLessThan(10_000);
});
