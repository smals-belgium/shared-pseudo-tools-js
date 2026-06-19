import { PseudoService } from "./pseudonymization.service";

describe("PseudoService (FULL MOCK STABLE)", () => {
  let service: PseudoService;

  let helperMock: any;
  let valueFactory: any;
  let transitFactory: any;

  beforeEach(() => {
    const fakeValue = (v: string) => ({
      asString: () => v,
      asShortString: () => `p-${v}`,
      asBytes: () => new Uint8Array([1, 2, 3]),
      pseudonymize: () => Promise.resolve(fakePseudonym(`p-${v}`)),
      identify: () => Promise.resolve(fakeValue(v)),
    });

    const fakePseudonym = (v: string) => ({
      asShortString: () => v,
      asString: () => v,
      transitInfo: {
        headers: { get: () => "9999999999" },
        asString: () => "token.payload",
      },
      identify: () => Promise.resolve(fakeValue(v.replace("p-", ""))),
    });

    valueFactory = {
      fromString: jest.fn((v: string) => fakeValue(v)),
      fromArray: jest.fn(() => fakeValue("byte")),
      multiple: jest.fn(() => ({
        pushPoint: jest.fn(),
      })),
    };

    transitFactory = {
      fromSec1AndTransitInfo: jest.fn((v: string) => fakePseudonym(`p-${v}`)),
      multiple: jest.fn(() => ({
        pushPoint: jest.fn(),
      })),
    };

    helperMock = {
      createDomain: jest.fn(() => ({
        valueFactory,
        pseudonymInTransitFactory: transitFactory,
      })),
    };

    service = new PseudoService(helperMock, {
      domain: "test",
      curve: "curve",
      audience: "aud",
      bufferSize: 10,
      cache: undefined,
    });
  });

  it("should create domain", () => {
    expect(helperMock.createDomain).toHaveBeenCalled();
  });
});
