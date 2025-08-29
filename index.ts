export type HLC = string;

export type Subject = string;
export type Predicate = string;
export type Value = string | number | boolean | Date | null;
export type Ref = { id: string };
export type RefMany = Ref[];

export type Object = Value | Ref | RefMany;
export type Triple = [Subject, Predicate, Object, HLC];
