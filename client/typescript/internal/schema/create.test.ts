import { describe, expect, expectTypeOf, it } from "bun:test";
import { createSchema } from "./create.js";
import { t } from "./t.js";
import type { Field } from "./types.js";

describe("createSchema", () => {
	describe("shared scope", () => {
		it("infers entity types in shared scope", () => {
			const schema = createSchema({
				shared: {
					users: {
						name: t.string({ fallback: "" }),
						age: t.number({ optional: true }),
						isAdult: t.boolean({ fallback: false }),
					},
				},
			});
			expectTypeOf(schema.shared).toEqualTypeOf<{
				users: {
					name: Field<string, false>;
					age: Field<number, true>;
					isAdult: Field<boolean, false>;
				};
			}>();
			expectTypeOf(schema.entities).toEqualTypeOf<{
				users: {
					name: Field<string, false>;
					age: Field<number, true>;
					isAdult: Field<boolean, false>;
				};
			}>();
		});

		it("infers multiple entity types in shared scope", () => {
			const schema = createSchema({
				shared: {
					users: {
						name: t.string({ fallback: "" }),
						age: t.number({ optional: true }),
						isAdult: t.boolean({ fallback: false }),
					},
					posts: {
						name: t.string({ optional: true }),
					},
				},
			});
			expectTypeOf(schema.shared).toEqualTypeOf<{
				users: {
					name: Field<string, false>;
					age: Field<number, true>;
					isAdult: Field<boolean, false>;
				};
				posts: {
					name: Field<string, true>;
				};
			}>();
		});
	});

	describe("user scope", () => {
		it("infers entity types in user scope", () => {
			const schema = createSchema({
				user: {
					preferences: {
						theme: t.string({ fallback: "light" }),
						notifications: t.boolean({ fallback: true }),
					},
				},
			});
			expectTypeOf(schema.user).toEqualTypeOf<{
				preferences: {
					theme: Field<string, false>;
					notifications: Field<boolean, false>;
				};
			}>();
			expectTypeOf(schema.entities).toEqualTypeOf<{
				preferences: {
					theme: Field<string, false>;
					notifications: Field<boolean, false>;
				};
			}>();
		});
	});

	describe("combined shared and user scopes", () => {
		it("infers types for both scopes", () => {
			const schema = createSchema({
				shared: {
					posts: {
						title: t.string({ fallback: "" }),
					},
				},
				user: {
					drafts: {
						content: t.string({ optional: true }),
					},
				},
			});
			expectTypeOf(schema.shared).toEqualTypeOf<{
				posts: {
					title: Field<string, false>;
				};
			}>();
			expectTypeOf(schema.user).toEqualTypeOf<{
				drafts: {
					content: Field<string, true>;
				};
			}>();
			expectTypeOf(schema.entities).toEqualTypeOf<{
				posts: {
					title: Field<string, false>;
				};
				drafts: {
					content: Field<string, true>;
				};
			}>();
		});

		it("flattens entities from both scopes", () => {
			const schema = createSchema({
				shared: {
					users: {
						name: t.string({ fallback: "" }),
					},
				},
				user: {
					settings: {
						theme: t.string({ fallback: "dark" }),
					},
				},
			});
			expect(schema.entities).toEqual({
				users: { name: expect.any(Object) },
				settings: { theme: expect.any(Object) },
			});
		});
	});

	describe("ref types", () => {
		it("infers ref types in shared scope", () => {
			const schema = createSchema({
				shared: {
					posts: {
						authorId: t.ref("users"),
					},
				},
			});
			expectTypeOf(schema.shared.posts.authorId).toEqualTypeOf<
				Field<string, true>
			>();
		});

		it("infers ref types in user scope", () => {
			const schema = createSchema({
				user: {
					bookmarks: {
						postId: t.ref("posts"),
					},
				},
			});
			expectTypeOf(schema.user.bookmarks.postId).toEqualTypeOf<
				Field<string, true>
			>();
		});
	});

	describe("entity name uniqueness", () => {
		it("throws when entity name appears in both shared and user scopes", () => {
			expect(() =>
				createSchema({
					shared: {
						users: {
							name: t.string({ fallback: "" }),
						},
					},
					user: {
						users: {
							email: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow(
				"Entity name 'users' must be unique across shared and user scopes",
			);
		});

		it("allows same entity name in different schemas", () => {
			const schema1 = createSchema({
				shared: {
					users: {
						name: t.string({ fallback: "" }),
					},
				},
			});
			const schema2 = createSchema({
				user: {
					users: {
						email: t.string({ fallback: "" }),
					},
				},
			});
			expect(schema1.shared.users).toBeDefined();
			expect(schema2.user.users).toBeDefined();
		});
	});

	describe("reserved fields are not allowed", () => {
		it("id is not allowed in shared scope", () => {
			expect(() =>
				createSchema({
					shared: {
						users: {
							id: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'id' is not allowed");
		});

		it("id is not allowed in user scope", () => {
			expect(() =>
				createSchema({
					user: {
						users: {
							id: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'id' is not allowed");
		});

		it("createTime is not allowed", () => {
			expect(() =>
				createSchema({
					shared: {
						users: {
							createTime: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'createTime' is not allowed");
		});

		it("updateTime is not allowed", () => {
			expect(() =>
				createSchema({
					shared: {
						users: {
							updateTime: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'updateTime' is not allowed");
		});

		it("creator is not allowed", () => {
			expect(() =>
				createSchema({
					shared: {
						users: {
							creator: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'creator' is not allowed");
		});
	});

	describe("schema immutability", () => {
		it("returns a frozen schema", () => {
			const schema = createSchema({
				shared: {
					users: {
						name: t.string({ fallback: "" }),
					},
				},
			});
			expect(Object.isFrozen(schema)).toBe(true);
		});
	});

	describe("legacy format backward compatibility", () => {
		it("accepts legacy { entities: {...} } format", () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ fallback: "" }),
					},
					posts: {
						title: t.string({ fallback: "" }),
					},
				},
			});
			expect(Object.keys(schema.entities)).toEqual(["users", "posts"]);
		});

		it("treats legacy entities as shared scope", () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ fallback: "" }),
					},
				},
			});
			expect(schema.shared).toEqual({
				users: { name: expect.any(Object) },
			});
			expect(schema.entities).toEqual({
				users: { name: expect.any(Object) },
			});
		});

		it("sets user scope to empty object for legacy format", () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ fallback: "" }),
					},
				},
			});
			expect(schema.user).toEqual({});
		});

		it("infers entity types for legacy format", () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ fallback: "" }),
						age: t.number({ optional: true }),
					},
				},
			});
			expectTypeOf(schema.shared).toEqualTypeOf<{
				users: {
					name: Field<string, false>;
					age: Field<number, true>;
				};
			}>();
			expectTypeOf(schema.entities).toEqualTypeOf<{
				users: {
					name: Field<string, false>;
					age: Field<number, true>;
				};
			}>();
		});

		it("validates reserved fields in legacy format", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							id: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'id' is not allowed");
		});

		it("validates reserved fields (createTime) in legacy format", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							createTime: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'createTime' is not allowed");
		});

		it("returns a frozen schema for legacy format", () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ fallback: "" }),
					},
				},
			});
			expect(Object.isFrozen(schema)).toBe(true);
		});

		it("supports ref types in legacy format", () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ fallback: "" }),
					},
					posts: {
						authorId: t.ref("users"),
					},
				},
			});
			expectTypeOf(schema.entities.posts.authorId).toEqualTypeOf<
				Field<string, true>
			>();
		});
	});
});
