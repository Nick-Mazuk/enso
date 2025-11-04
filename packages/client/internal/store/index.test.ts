import { describe, expect, it } from "bun:test";
import { Store } from "./index";
import { movies } from "./testdata/movies";
import { type Datom, Field, Id, Value, Variable } from "./types";

const sortResult = (result: (Datom | undefined)[][], index: number) => {
	return result.sort((a, b) => {
		const fieldA = a[index];
		if (fieldA === undefined) return -1;
		const fieldB = b[index];
		if (fieldB === undefined) return 1;
		return String(fieldA).localeCompare(String(fieldB));
	});
};

describe("add", () => {
	it("adds a triple to an empty index", () => {
		const store = new Store();
		store.add([Id("x"), Field("y"), Value("z")]);
	});
});

describe("deleteAllById", () => {
	it("deletes all triples with the given id", () => {
		const store = new Store();
		store.add(...movies);
		store.deleteAllById(Id("100"));
		// There are 2 triples with the id 100:
		expect(store.size()).toBe(movies.length - 2);
	});
});

describe("Store.query with optional patterns", () => {
	it("should return documents even if optional fields are missing", () => {
		const store = new Store();

		// User 1: Has 'name' (required) and 'age' (optional)
		store.add([Id("id-1"), Field("users/name"), Value("John Doe")]);
		store.add([Id("id-1"), Field("users/age"), Value(30)]);

		// User 2: Has 'name' (required) but NO 'age' (optional)
		store.add([Id("id-2"), Field("users/name"), Value("Jane Smith")]);

		// User 3: Has 'age' but no 'name' (should be filtered out by 'where')
		store.add([Id("id-3"), Field("users/age"), Value(40)]);

		const result = store.query({
			find: [Variable("id"), Variable("name"), Variable("age")],
			where: [[Variable("id"), Field("users/name"), Variable("name")]],
			optional: [[Variable("id"), Field("users/age"), Variable("age")]],
		});

		const sortedResult = sortResult(result, 1);

		expect(sortedResult).toEqual([
			[
				Id("id-2"), // id
				Value("Jane Smith"), // name
				undefined, // age
			],
			[
				Id("id-1"), // id
				Value("John Doe"), // name
				Value(30), // age
			],
		]);
	});

	it("should handle multiple optional fields with mixed matches", () => {
		const store = new Store();
		// Doc 1: all fields
		store.add([Id("id-1"), Field("users/name"), Value("Alice")]);
		store.add([Id("id-1"), Field("users/age"), Value(30)]);
		store.add([Id("id-1"), Field("users/dept"), Value("Engineering")]);
		// Doc 2: no dept
		store.add([Id("id-2"), Field("users/name"), Value("Bob")]);
		store.add([Id("id-2"), Field("users/age"), Value(40)]);
		// Doc 3: no age
		store.add([Id("id-3"), Field("users/name"), Value("Charlie")]);
		store.add([Id("id-3"), Field("users/dept"), Value("Marketing")]);
		// Doc 4: no age, no dept
		store.add([Id("id-4"), Field("users/name"), Value("David")]);

		const result = store.query({
			find: [
				Variable("id"),
				Variable("name"),
				Variable("age"),
				Variable("dept"),
			],
			where: [[Variable("id"), Field("users/name"), Variable("name")]],
			optional: [
				[Variable("id"), Field("users/age"), Variable("age")],
				[Variable("id"), Field("users/dept"), Variable("dept")],
			],
		});

		const sortedResult = sortResult(result, 1);
		expect(sortedResult).toEqual([
			// id, name, age, dept
			[Id("id-1"), Value("Alice"), Value(30), Value("Engineering")],
			[Id("id-2"), Value("Bob"), Value(40), undefined],
			[Id("id-3"), Value("Charlie"), undefined, Value("Marketing")],
			[Id("id-4"), Value("David"), undefined, undefined],
		]);
	});

	it("should return an empty array if the 'where' clause matches nothing", () => {
		const store = new Store();
		store.add([Id("id-1"), Field("users/name"), Value("Alice")]);
		store.add([Id("id-1"), Field("users/age"), Value(30)]);

		const result = store.query({
			find: [Variable("id"), Variable("name"), Variable("age")],
			where: [
				[Variable("id"), Field("users/name"), Value("Bob")], // This won't match
			],
			optional: [[Variable("id"), Field("users/age"), Variable("age")]],
		});

		expect(result.length).toBe(0);
	});
});

describe("movies example", () => {
	it("adds the movies example to the store", () => {
		const store = new Store();
		store.add(...movies);
		expect(store.size()).toBe(movies.length);
	});

	it("When was Alien released?", () => {
		const store = new Store();
		store.add(...movies);
		const result = store.query({
			find: [Variable("year")],
			where: [
				[Variable("id"), Field("movie/title"), Value("Alien")],
				[Variable("id"), Field("movie/year"), Variable("year")],
			],
		});
		expect(result).toEqual([[Value(1979)]]);
	});

	it("Who directed the Terminator?", () => {
		const store = new Store();
		store.add(...movies);
		const result = store.query({
			find: [Variable("directorName")],
			where: [
				[Variable("id"), Field("movie/title"), Value("The Terminator")],
				[Variable("id"), Field("movie/director"), Variable("directorId")],
				[
					Variable("directorId"),
					Field("person/name"),
					Variable("directorName"),
				],
			],
		});
		expect(result).toEqual([[Value("James Cameron")]]);
	});

	it("What do I know about the entity with the id `200`?", () => {
		const store = new Store();
		store.add(...movies);
		const result = store.query({
			find: [Variable("attribute"), Variable("value")],
			where: [[Id("200"), Variable("attribute"), Variable("value")]],
		});
		expect(result).toEqual([
			[Field("movie/title"), Value("The Terminator")],
			[Field("movie/year"), Value(1984)],
			[Field("movie/director"), Id("100")],
			[Field("movie/cast"), Id("101")],
			[Field("movie/cast"), Id("102")],
			[Field("movie/cast"), Id("103")],
			[Field("movie/sequel"), Id("207")],
		]);
	});

	it("Which directors shot Arnold for which movies?", () => {
		const store = new Store();
		store.add(...movies);
		const result = store.query({
			find: [Variable("directorName"), Variable("movieTitle")],
			where: [
				[
					Variable("arnoldId"),
					Field("person/name"),
					Value("Arnold Schwarzenegger"),
				],
				[Variable("movieId"), Field("movie/cast"), Variable("arnoldId")],
				[Variable("movieId"), Field("movie/title"), Variable("movieTitle")],
				[Variable("movieId"), Field("movie/director"), Variable("directorId")],
				[
					Variable("directorId"),
					Field("person/name"),
					Variable("directorName"),
				],
			],
		});
		expect(result).toEqual([
			[Value("James Cameron"), Value("The Terminator")],
			[Value("John McTiernan"), Value("Predator")],
			[Value("Mark L. Lester"), Value("Commando")],
			[Value("James Cameron"), Value("Terminator 2: Judgment Day")],
			[Value("Jonathan Mostow"), Value("Terminator 3: Rise of the Machines")],
		]);
	});
});
