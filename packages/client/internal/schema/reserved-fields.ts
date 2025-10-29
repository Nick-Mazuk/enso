export const reservedFields = [
	"id",
	"createTime",
	"updateTime",
	"creator",
] as const;
export type ReservedField = (typeof reservedFields)[number];
