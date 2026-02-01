/**
 * Store module for data persistence.
 *
 * This module exports the production NetworkStore and shared types.
 * For testing, use MockStore from ./testing/index.js instead.
 */
export { NetworkStore } from "./network-store.js";
export type {
	Datom,
	Filter,
	Query,
	QueryPattern,
	QueryVariable,
	StoreInterface,
	Triple,
} from "./types.js";
export {
	Field,
	Id,
	isVariable,
	Value,
	Variable,
} from "./types.js";
