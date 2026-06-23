import typescript from "@rollup/plugin-typescript";
import generatePackageJSON from "rollup-plugin-generate-package-json";
import dts from "rollup-plugin-dts";
import copy from "rollup-plugin-copy";

export default [
  {
    input: "src/index.ts",
    output: [
      {
        file: "dist/index.js",
        format: "esm",
        sourcemap: true,
      },
      {
        file: "dist/index.cjs",
        format: "cjs",
        sourcemap: true,
      },
    ],
    plugins: [
      typescript({
        tsconfig: "./tsconfig.json",
      }),
      generatePackageJSON({
        outputFolder: "dist",
        baseContents: (pkg) => ({
          name: pkg.name,
          version: pkg.version,
          description: pkg.description,
          license: pkg.license,
          author: pkg.author,

          type: "module",

          main: "./index.cjs",
          module: "./index.js",
          types: "./index.d.ts",

          exports: {
            ".": {
              types: "./index.d.ts",
              import: "./index.js",
              require: "./index.cjs",
              default: "./index.js",
            },
          },

          dependencies: pkg.dependencies,
        }),
      }),
      copy({
        targets: [
          { src: "NPM.md", dest: "dist", rename: "README.md" },
          { src: "LICENSE.md", dest: "dist", rename: "LICENSE" },
        ],
      }),
    ],
    external: [
      "rxjs",
      "@isaacs/ttlcache",
      "@smals-belgium-shared/pseudo-helper",
      "js-base64",
      "luxon",
    ],
  },
  {
    input: "dist/types/src/index.d.ts",
    output: [
      {
        file: "dist/index.d.ts",
        format: "es",
      },
    ],
    plugins: [dts()],
  },
];
