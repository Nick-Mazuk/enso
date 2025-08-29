export class HLC {
  readonly time: Date;
  readonly counter: number;

  constructor(time: Date, counter: number) {
    if (counter < 0 || counter > 0xffff) {
      throw new Error("HLC counter is out of range (0-65535).");
    }
    this.time = time;
    this.counter = counter;
  }

  static fromString(hlcString: HLC | string): HLC | undefined {
    if (typeof hlcString !== "string" || !hlcString.includes("-")) {
      return undefined;
    }
    const parts = hlcString.split("-");
    const timeString = parts.slice(0, parts.length - 1).join("-");
    const counterString = parts[parts.length - 1];

    if (!timeString || !counterString || counterString.length !== 4) {
      return undefined;
    }

    const time = new Date(timeString);
    const counter = parseInt(counterString, 16);

    if (isNaN(time.getTime()) || isNaN(counter)) {
      return undefined;
    }

    return new HLC(time, counter);
  }

  increment(): HLC {
    if (this.counter === 0xffff) {
      const newTime = new Date(this.time.getTime() + 1);
      return new HLC(newTime, 0);
    }
    return new HLC(this.time, this.counter + 1);
  }

  compare(other: HLC): -1 | 0 | 1 {
    if (this.time.getTime() < other.time.getTime()) {
      return -1;
    }
    if (this.time.getTime() > other.time.getTime()) {
      return 1;
    }
    if (this.counter < other.counter) {
      return -1;
    }
    if (this.counter > other.counter) {
      return 1;
    }
    return 0;
  }

  toString(): string {
    return `${this.time.toISOString()}-${this.counter
      .toString(16)
      .toUpperCase()
      .padStart(4, "0")}`;
  }
}
