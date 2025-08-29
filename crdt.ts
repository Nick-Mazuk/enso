import type { Value } from "./store";

const STRING_PREFIX = "s:";
const NUMBER_PREFIX = "n:";
const BOOLEAN_PREFIX = "b:";
const DATE_PREFIX = "d:";
const NULL_PREFIX = "l:";

export const encode = (value: Value): string => {
  if (typeof value === "string") {
    return `${STRING_PREFIX}${value}`;
  }
  if (typeof value === "number") {
    return `${NUMBER_PREFIX}${value}`;
  }
  if (typeof value === "boolean") {
    return `${BOOLEAN_PREFIX}${value}`;
  }
  if (value instanceof Date) {
    return `${DATE_PREFIX}${value.toISOString()}`;
  }
  if (value === null) {
    return NULL_PREFIX;
  }
  throw new Error(`Unsupported value type: ${typeof value}`);
};

export const decode = (encoded: string): Value => {
  const prefix = encoded.slice(0, 2);
  const value = encoded.slice(2);

  if (prefix === STRING_PREFIX) {
    return value;
  }
  if (prefix === NUMBER_PREFIX) {
    return Number(value);
  }
  if (prefix === BOOLEAN_PREFIX) {
    return value === "true";
  }
  if (prefix === DATE_PREFIX) {
    return new Date(value);
  }
  if (encoded === NULL_PREFIX) {
    return null;
  }
  throw new Error(`Unsupported encoded value: ${encoded}`);
};
