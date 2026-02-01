import { describe, expect, it } from "bun:test";
import { type Datom, Field, Id, Value, Variable } from "../types.js";
import { MockStore } from "./mock-store.js";
import { movies } from "./testdata/movies.js";

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
	it("adds a triple to an empty index", async () => {
		const store = new MockStore();
		await store.add([Id("x"), Field("y"), Value("z")]);
		expect(store.size()).toBe(1);
	});
});

describe("deleteAllById", () => {
	it("deletes all triples with the given id", async () => {
		const store = new MockStore();
		await store.add(...movies);
		await store.deleteAllById(Id("100"));
		// There are 2 triples with the id 100:
		expect(store.size()).toBe(movies.length - 2);
	});
});

describe("MockStore.query with optional patterns", () => {
	it("should return documents even if optional fields are missing", async () => {
		const store = new MockStore();

		// User 1: Has 'name' (required) and 'age' (optional)
		await store.add([Id("id-1"), Field("users/name"), Value("John Doe")]);
		await store.add([Id("id-1"), Field("users/age"), Value(30)]);

		// User 2: Has 'name' (required) but NO 'age' (optional)
		await store.add([Id("id-2"), Field("users/name"), Value("Jane Smith")]);

		// User 3: Has 'age' but no 'name' (should be filtered out by 'where')
		await store.add([Id("id-3"), Field("users/age"), Value(40)]);

		const result = await store.query({
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

	it("should handle multiple optional fields with mixed matches", async () => {
		const store = new MockStore();
		// Doc 1: all fields
		await store.add([Id("id-1"), Field("users/name"), Value("Alice")]);
		await store.add([Id("id-1"), Field("users/age"), Value(30)]);
		await store.add([Id("id-1"), Field("users/dept"), Value("Engineering")]);
		// Doc 2: no dept
		await store.add([Id("id-2"), Field("users/name"), Value("Bob")]);
		await store.add([Id("id-2"), Field("users/age"), Value(40)]);
		// Doc 3: no age
		await store.add([Id("id-3"), Field("users/name"), Value("Charlie")]);
		await store.add([Id("id-3"), Field("users/dept"), Value("Marketing")]);
		// Doc 4: no age, no dept
		await store.add([Id("id-4"), Field("users/name"), Value("David")]);

		const result = await store.query({
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

	it("should return an empty array if the 'where' clause matches nothing", async () => {
		const store = new MockStore();
		await store.add([Id("id-1"), Field("users/name"), Value("Alice")]);
		await store.add([Id("id-1"), Field("users/age"), Value(30)]);

		const result = await store.query({
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
	it("adds the movies example to the store", async () => {
		const store = new MockStore();
		await store.add(...movies);
		expect(store.size()).toBe(movies.length);
	});

	it("When was Alien released?", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
			find: [Variable("year")],
			where: [
				[Variable("id"), Field("movie/title"), Value("Alien")],
				[Variable("id"), Field("movie/year"), Variable("year")],
			],
		});
		expect(result).toEqual([[Value(1979)]]);
	});

	it("Who directed the Terminator?", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
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

	it("What do I know about the entity with the id `200`?", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
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

	it("Which directors shot Arnold for which movies?", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
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

describe("MockStore.query with filters", () => {
	const yearVar = Variable("year");
	const titleVar = Variable("title");
	const idVar = Variable("id");

	it("should filter numbers: greaterThan", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
			find: [titleVar, yearVar],
			where: [
				[idVar, Field("movie/title"), titleVar],
				[idVar, Field("movie/year"), yearVar],
			],
			filters: [
				{
					selector: yearVar,
					filter: (year) => typeof year === "number" && year > 1990,
				},
			],
		});

		const sorted = sortResult(result, 0);
		expect(sorted).toEqual([
			[Value("Braveheart"), Value(1995)],
			[Value("Lethal Weapon 3"), Value(1992)],
			[Value("Terminator 2: Judgment Day"), Value(1991)],
			[Value("Terminator 3: Rise of the Machines"), Value(2003)],
		]);
	});

	it("should filter numbers: lessThanOrEqual", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
			find: [titleVar, yearVar],
			where: [
				[idVar, Field("movie/title"), titleVar],
				[idVar, Field("movie/year"), yearVar],
			],
			filters: [
				{
					selector: yearVar,
					filter: (year) => typeof year === "number" && year <= 1982,
				},
			],
		});

		const sorted = sortResult(result, 0);
		expect(sorted).toEqual([
			[Value("Alien"), Value(1979)],
			[Value("First Blood"), Value(1982)],
			[Value("Mad Max"), Value(1979)],
			[Value("Mad Max 2"), Value(1981)],
		]);
	});

	it("should filter strings: startsWith", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
			find: [titleVar],
			where: [[idVar, Field("movie/title"), titleVar]],
			filters: [
				{
					selector: titleVar,
					filter: (title) =>
						typeof title === "string" && title.startsWith("Terminator"),
				},
			],
		});

		const sorted = sortResult(result, 0);
		expect(sorted).toEqual([
			[Value("Terminator 2: Judgment Day")],
			[Value("Terminator 3: Rise of the Machines")],
		]);
		// Note: "The Terminator" will not match.
	});

	it("should filter strings: contains", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const nameVar = Variable("name");
		const result = await store.query({
			find: [nameVar],
			where: [[idVar, Field("person/name"), nameVar]],
			filters: [
				{
					selector: nameVar,
					filter: (name) =>
						typeof name === "string" && name.includes("Cameron"),
				},
			],
		});
		expect(result).toEqual([[Value("James Cameron")]]);
	});

	it("should combine multiple filters (AND)", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const result = await store.query({
			find: [titleVar, yearVar],
			where: [
				[idVar, Field("movie/title"), titleVar],
				[idVar, Field("movie/year"), yearVar],
			],
			filters: [
				{
					selector: yearVar,
					filter: (year) => typeof year === "number" && year > 1985,
				},
				{
					selector: titleVar,
					filter: (title) => typeof title === "string" && title.startsWith("L"),
				},
			],
		});

		const sorted = sortResult(result, 0);
		expect(sorted).toEqual([
			[Value("Lethal Weapon"), Value(1987)],
			[Value("Lethal Weapon 2"), Value(1989)],
			[Value("Lethal Weapon 3"), Value(1992)],
		]);
	});

	it("should apply filters to optional fields", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const nameVar = Variable("name");
		const deathVar = Variable("death");
		const result = await store.query({
			find: [nameVar, deathVar],
			where: [[idVar, Field("person/name"), nameVar]],
			optional: [[idVar, Field("person/death"), deathVar]],
			filters: [
				// Find people who died, but only after 2000
				{
					selector: deathVar,
					filter: (death) => {
						if (death === undefined) return true;
						return typeof death === "string" && death > "2000-01-01T00:00:00Z";
					},
				},
			],
		});

		// This should return all living people (deathVar = undefined)
		// AND people who died after 2000.
		const peopleWhoDiedAfter2000 = result.filter((r) => r[1] !== undefined);
		const sorted = sortResult(peopleWhoDiedAfter2000, 0);

		expect(sorted).toEqual([
			[Value("Charles Napier"), Value("2011-10-05T00:00:00Z")],
			[Value("George P. Cosmatos"), Value("2005-04-19T00:00:00Z")],
			[Value("Richard Crenna"), Value("2003-01-17T00:00:00Z")],
		]);
		// Ensure we still get living people
		expect(result.length).toBeGreaterThan(3);
	});

	it("should filter booleans", async () => {
		const boolStore = new MockStore();
		const nameVar = Variable("name");
		const awesomeVar = Variable("awesome");
		await boolStore.add([Id("1"), Field("person/name"), Value("Nick")]);
		await boolStore.add([Id("1"), Field("person/isAwesome"), Value(true)]);
		await boolStore.add([Id("2"), Field("person/name"), Value("SomeoneElse")]);
		await boolStore.add([Id("2"), Field("person/isAwesome"), Value(false)]);

		const result = await boolStore.query({
			find: [nameVar],
			where: [
				[idVar, Field("person/name"), nameVar],
				[idVar, Field("person/isAwesome"), awesomeVar],
			],
			filters: [
				{
					selector: awesomeVar,
					filter: (isAwesome) =>
						typeof isAwesome === "boolean" && isAwesome === true,
				},
			],
		});

		expect(result).toEqual([[Value("Nick")]]);
	});
});

describe("MockStore.query with whereNot", () => {
	const idVar = Variable("id");

	it("should filter out results: `isUndefined` equivalent", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const nameVar = Variable("name");
		const deathVar = Variable("death");

		// Find people who are *not* dead (i.e., have no person/death triple)
		const result = await store.query({
			find: [nameVar],
			where: [[idVar, Field("person/name"), nameVar]],
			whereNot: [[idVar, Field("person/death"), deathVar]],
		});

		const totalPeople = movies.filter((m) => m[1] === "person/name").length;
		const deadPeople = movies.filter((m) => m[1] === "person/death").length;
		expect(result.length).toBe(totalPeople - deadPeople);
	});

	it("should filter out results based on a specific value", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const titleVar = Variable("title");
		const result = await store.query({
			find: [titleVar],
			where: [[idVar, Field("movie/title"), titleVar]],
			whereNot: [[idVar, Field("movie/title"), Value("Alien")]],
		});

		const totalMovies = movies.filter((m) => m[1] === "movie/title").length;
		expect(result.length).toBe(totalMovies - 1);
		expect(result.some((r) => r[0] === "Alien")).toBe(false);
	});

	it("should interact with variable bindings from `where`", async () => {
		const store = new MockStore();
		await store.add(...movies);
		const directorNameVar = Variable("directorName");
		const directorIdVar = Variable("directorId");

		// Find all directors who directed a movie,
		// *unless* they directed "The Terminator"
		const result = await store.query({
			find: [directorNameVar],
			where: [
				[Variable("movieId"), Field("movie/director"), directorIdVar],
				[directorIdVar, Field("person/name"), directorNameVar],
			],
			whereNot: [
				[Variable("otherMovie"), Field("movie/director"), directorIdVar],
				[Variable("otherMovie"), Field("movie/title"), Value("The Terminator")],
			],
		});
		const nonJamesCameronDirectors = new Set(result.map((item) => item[0]));

		const allDirectors = await store.query({
			find: [directorNameVar],
			where: [
				[Variable("movieId"), Field("movie/director"), directorIdVar],
				[directorIdVar, Field("person/name"), directorNameVar],
			],
		});
		const allUniqueDirectors = new Set(allDirectors.map((item) => item[0]));

		// We expect all directors *except* James Cameron
		expect(nonJamesCameronDirectors.size).toBe(allUniqueDirectors.size - 1);
		expect(result.some((r) => r[0] === "James Cameron")).toBe(false);
	});
});
