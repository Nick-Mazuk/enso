import { describe, expect, it } from "bun:test";
import { Store } from "./index";
import { movies } from "./testdata/movies";
import { Field, Id, Value, Variable } from "./types";

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
