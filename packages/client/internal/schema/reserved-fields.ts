export const reservedFields = [
	"id",
	"createdAt",
	"updatedAt",
	"createdBy",
] as const;
export type ReservedField = (typeof reservedFields)[number];
