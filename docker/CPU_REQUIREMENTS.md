# Container CPU requirements

The locked images target x86-64 Sandy Bridge CPUs with AVX, without AVX2, FMA
or BMI2. Polars selects its compatibility runtime. Full image compatibility
requires execution of the relevant lane on the target CPU; the dependency
evidence below does not establish a complete instruction-set baseline.

## Polars selection

The [installation guide][polars-guide] specifies `polars[rtcompat]` for older
CPUs. The [1.43.1 metadata][polars-metadata] declares both an unconditional
`polars-runtime-32==1.43.1` dependency and
`polars-runtime-compat==1.43.1` for the `rtcompat` extra. Both distributions
therefore remain in the shared lock file.

The `polars/_plr.py` loader in the [1.43.1 wheel][polars-loader] tries
installed runtimes in this order:
compatibility, 64-bit index, default 32-bit index. This is package preference,
not CPU feature dispatch. Its CPU check warns about missing features; it does
not switch runtimes. With the compatibility extra installed, no environment
variable is necessary. `POLARS_FORCE_PKG` and `POLARS_PREFER_PKG` can override
this selection and should remain unset.

All Python images install the versions in the shared lock file. Slim and Spark
use a non-editable installation; research uses an editable installation. The
compatibility runtime is a core dependency, so it applies with or without the
optional research embeddings.

Polars 1.43.1 and its runtimes are yanked on PyPI.

## Native dependencies

“Dispatched” describes optional AVX2 code selected at run time on supporting
CPUs; it does not establish the baseline of every bundled native library.
“No (inferred)” distinguishes portable build configuration from an explicit vendor CPU guarantee. Wheel platform
tags describe architecture and ABI, not a complete instruction-set manifest.

| Dependency in lock/image | AVX2 at import | AVX2 at run time | Evidence and limits |
| --- | --- | --- | --- |
| Polars 1.43.1 | Default: yes; compat: no | Compat: no | [Guide][polars-guide], [metadata][polars-metadata], [loader][polars-loader]. |
| DuckDB 1.5.5 | No | No | Official [platform requirements][duckdb] and [portability FAQ][duckdb-faq]; includes official extension platforms. |
| PyArrow 19.0.1 | Undetermined | Dispatch controls documented | [Arrow SIMD settings][arrow] describe runtime controls, not the exact wheel baseline. Target-CPU execution required. |
| deltalake 1.6.2 | No (inferred) | No (inferred) | [Release build][delta-build] targets generic x86-64; [build guide][delta-guide] distinguishes portable from Haswell/native builds. |
| NumPy 2.2.5 / 2.5.4 | Undetermined | Build-dependent dispatch | [SIMD build options][numpy] separate baseline and dispatch; wheel flags are undetermined. [OpenBLAS][openblas] supports Sandy Bridge. |
| SciPy 1.17.1 / 1.18.1 | No (inferred) | Dispatched | [Wheel build][scipy] and [toolchain][scipy-toolchain] use OpenBLAS; [OpenBLAS][openblas] supports Sandy Bridge. |
| scikit-learn 1.9.1 | No (inferred) | Dispatched through NumPy/SciPy | [Generic Linux wheel build][sklearn]; BLAS follows the NumPy/SciPy dependencies. |
| opencv-python-headless 4.14.0.94 | Undetermined | Build-dependent dispatch | [OpenCV configuration][opencv] documents defaults and options, not the exact wheel flags. Target-CPU execution required. |
| pypdfium2 5.14.0 | Undetermined | Undetermined | [Wheel metadata][pdfium] has generic manylinux x86-64 tags; these alone do not establish the bundled PDFium ISA baseline. |
| torch 2.14.1+cpu | No (inferred) | Operator-dependent | [Tagged build][torch-build] and [dispatch guide][torch-dispatch] describe CPU dispatch, not all embedding-path library or operator baselines. |
| tokenizers 0.23.3 | No (inferred) | No (inferred) | [Tagged wheel build][tokenizers] uses generic x86-64, with no Linux target-cpu/native RUSTFLAGS override. |
| sentence-transformers 5.7.0 | No of its own | Through dependencies | [Metadata][sentence] supplies a `py3-none-any` wheel; native requirements belong to Torch, tokenizers, NumPy and SciPy. |
| psycopg-binary 3.3.6 | No (inferred) | No (inferred) | [Tagged build][psycopg] uses manylinux2014 x86-64 without an AVX2 target override; bundled library ISA is not specified in wheel tags. |
| Bun 1.4.0 builder | No | No baseline requirement | The [tagged Dockerfile][bun-image] already downloads `x64-baseline`; [tagged docs][bun-docs] specify Nehalem/SSE4.2 for baseline. |
| Node 24 final web runtime | No (inferred) | Dispatched | [Supported x64 platform][node-build] and [V8 CPU detection][node-cpu] detect AVX/AVX2 separately. |

The exact bundled PDFium baseline, NumPy and OpenCV wheel build flags, and
Torch operators used by the embedding path require execution of the relevant
lane on the target CPU. Torch dispatch alone does not establish that path's
compatibility, including for specialized operators and downloaded models.
FMA and BMI2 requirements are also undetermined across the images; the AVX2
matrix does not establish their absence. End-to-end assurance requires target-CPU
execution with the bundled native libraries and DuckDB extensions. An environment
override cannot repair a binary that requires unsupported instructions.

[polars-guide]: https://docs.pola.rs/user-guide/installation/
[polars-metadata]: https://pypi.org/pypi/polars/1.43.1/json
[polars-loader]: https://files.pythonhosted.org/packages/f3/7c/d74a56d5afae8e91924aad91cba621b286a05307e88569ecab666b3055b3/polars-1.43.1-py3-none-any.whl
[duckdb]: https://duckdb.org/docs/current/operations_manual/installing_duckdb/requirements
[duckdb-faq]: https://duckdb.org/faq
[arrow]: https://arrow.apache.org/docs/19.0/cpp/env_vars.html
[delta-build]: https://github.com/delta-io/delta-rs/blob/python-v1.6.2/.github/workflows/python_release.yml
[delta-guide]: https://github.com/delta-io/delta-rs/blob/main/python/README.md
[numpy]: https://numpy.org/doc/stable/reference/simd/build-options.html
[openblas]: https://github.com/OpenMathLib/OpenBLAS/blob/develop/README.md
[scipy]: https://github.com/scipy/scipy/blob/v1.18.1/.github/workflows/wheels.yml
[scipy-toolchain]: https://docs.scipy.org/doc/scipy/dev/toolchain.html
[sklearn]: https://github.com/scikit-learn/scikit-learn/blob/1.9.1/.github/workflows/wheels.yml
[opencv]: https://docs.opencv.org/4.x/db/d05/tutorial_config_reference.html
[pdfium]: https://pypi.org/pypi/pypdfium2/5.14.0/json
[torch-build]: https://github.com/pytorch/pytorch/blob/v2.14.1/caffe2/CMakeLists.txt
[torch-dispatch]: https://github.com/pytorch/pytorch/blob/v2.14.1/aten/src/ATen/native/cpu/README.md
[tokenizers]: https://github.com/huggingface/tokenizers/blob/v0.23.3/.github/workflows/python-release.yml
[sentence]: https://pypi.org/pypi/sentence-transformers/5.7.0/json
[psycopg]: https://github.com/psycopg/psycopg/blob/3.3.6/.github/workflows/packages-bin.yml
[bun-image]: https://github.com/oven-sh/bun/blob/bun-v1.4.0/dockerhub/debian/Dockerfile
[bun-docs]: https://github.com/oven-sh/bun/blob/bun-v1.4.0/docs/installation.mdx
[node-build]: https://github.com/nodejs/node/blob/v24.x/BUILDING.md
[node-cpu]: https://github.com/nodejs/node/blob/v24.x/deps/v8/src/base/cpu.cc
