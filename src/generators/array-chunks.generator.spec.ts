import { arrayChunks } from "./array-chunks.generator";

describe("arrayChunks", () => {
  it("should split array into chunks of given size", () => {
    const result = [...arrayChunks([1, 2, 3, 4, 5], 2)];

    expect(result).toEqual([[1, 2], [3, 4], [5]]);
  });

  it("should return single chunk if size is larger than array", () => {
    const result = [...arrayChunks([1, 2, 3], 10)];

    expect(result).toEqual([[1, 2, 3]]);
  });

  it("should return empty array for empty input", () => {
    const result = [...arrayChunks([], 3)];

    expect(result).toEqual([]);
  });

  it("should handle chunk size equal to 1", () => {
    const result = [...arrayChunks([1, 2, 3], 1)];

    expect(result).toEqual([[1], [2], [3]]);
  });

  it("should not mutate original array", () => {
    const input = [1, 2, 3, 4];

    const snapshot = [...input];

    [...arrayChunks(input, 2)];

    expect(input).toEqual(snapshot);
  });
});
