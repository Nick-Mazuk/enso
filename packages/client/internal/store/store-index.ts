import type { Datom } from "./types";

// TODO: implement and use the index
export class StoreIndex<X extends Datom, Y extends Datom, Z extends Datom> {
	private index: Map<X, Map<Y, Z>> = new Map();

	add(x: X, y: Y, z: Z) {
		if (!this.index.has(x)) return true;
	}
}
