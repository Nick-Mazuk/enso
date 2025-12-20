/**
 * Assert that a condition is true. Use to enforce invariants.
 *
 * When `assert` is used, types in the condition will be narrowed.
 *
 * See also https://www.typescriptlang.org/docs/handbook/release-notes/typescript-3-7.html#assertion-functions
 *
 * @param condition The condition to check
 * @param message The error message to display if the condition is false. Ideally be descriptive enough to debug the error later.
 */
// Assertions cannot be arrow functions
// https://stackoverflow.com/questions/64297259/how-to-resolve-assertions-require-every-name-in-the-call-target-to-be-declared
export function assert(condition: boolean, message: string): asserts condition {
	if (!condition) {
		if (
			// biome-ignore lint/complexity/useLiteralKeys: required by TypeScript
			!(import.meta.env["PROD"] || import.meta.env["TEST"]) &&
			typeof window !== "undefined" &&
			typeof alert === "function"
		) {
			alert(message);
		}
		throw new Error(message);
	}
}
