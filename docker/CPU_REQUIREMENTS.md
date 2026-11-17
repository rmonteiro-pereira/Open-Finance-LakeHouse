# Container CPU requirements

Use the locked images on x86-64 Sandy Bridge CPUs with AVX, without AVX2, FMA
or BMI2. Polars needs its compatibility runtime. The other dependencies below
have portable builds or dispatch instructions according to CPU capabilities;
where upstream evidence is incomplete, the limitation is explicit.

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

All Python Dockerfiles use `uv sync --locked`. Slim and Spark install a
non-editable package into `/opt/venv`; research retains its editable `/app`
installation. The compatibility runtime is a core dependency, so it applies
with or without the optional research embeddings.

Polars 1.43.1 and its runtimes are yanked on PyPI. The existing version is
retained to avoid a package upgrade; the lock records exact artifacts and hashes.
A future upgrade needs its own compatibility review.

## Native dependencies

“Dispatched” means AVX2 is optional at run time, selected only on supporting
CPUs; it is not an import requirement. “No (inferred)” distinguishes portable
build configuration from an explicit vendor CPU guarantee. Wheel platform
tags describe architecture and ABI, not a complete instruction-set manifest.

| Dependency in lock/image | AVX2 at import | AVX2 at run time | Evidence and limits |
| --- | --- | --- | --- |
| Polars 1.43.1 | Default: yes; compat: no | Compat: no | [Guide][polars-guide], [metadata][polars-metadata], [loader][polars-loader]. |
| DuckDB 1.5.5 | No | No | Official [platform requirements][duckdb] and [portability FAQ][duckdb-faq]; includes official extension platforms. |
| PyArrow 19.0.1 | No | Dispatched | [Arrow SIMD settings][arrow]: SSE4.2 baseline, higher SIMD selected at run time. Sandy Bridge supplies SSE4.2. |
| deltalake 1.6.2 | No (inferred) | No (inferred) | [Release build][delta-build] targets generic x86-64; [build guide][delta-guide] distinguishes portable from Haswell/native builds. |
| NumPy 2.2.5 / 2.5.4 | No | Dispatched | [SIMD build options][numpy] separate baseline and dispatch; [OpenBLAS][openblas] includes Sandy Bridge dispatch. |
| SciPy 1.17.1 / 1.18.1 | No (inferred) | Dispatched | [Wheel build][scipy] and [toolchain][scipy-toolchain] use OpenBLAS; [OpenBLAS][openblas] supports Sandy Bridge. |
| scikit-learn 1.9.1 | No (inferred) | Dispatched through NumPy/SciPy | [Generic Linux wheel build][sklearn]; BLAS follows the NumPy/SciPy dependencies. |
| opencv-python-headless 4.14.0.94 | No | Dispatched | [OpenCV configuration][opencv]: SSE3 default baseline, optional dispatch kernels. |
| pypdfium2 5.14.0 | Undetermined | Undetermined | [Wheel metadata][pdfium] has generic manylinux x86-64 tags; these alone do not establish the bundled PDFium ISA baseline. |
| torch 2.14.1+cpu | No (inferred) | Dispatched; operator-dependent | [Tagged build][torch-build] separates non-AVX and AVX objects; [dispatch guide][torch-dispatch] includes default kernels. |
| tokenizers 0.23.3 | No (inferred) | No (inferred) | [Tagged wheel build][tokenizers] uses generic x86-64, with no Linux target-cpu/native RUSTFLAGS override. |
| sentence-transformers 5.7.0 | No of its own | Through dependencies | [Metadata][sentence] supplies a `py3-none-any` wheel; native requirements belong to Torch, tokenizers, NumPy and SciPy. |
| psycopg-binary 3.3.6 | No (inferred) | No (inferred) | [Tagged build][psycopg] uses manylinux2014 x86-64 without an AVX2 target override; bundled library ISA is not specified in wheel tags. |
| Bun 1.4.0 builder | No | No baseline requirement | The [tagged Dockerfile][bun-image] already downloads `x64-baseline`; [tagged docs][bun-docs] specify Nehalem/SSE4.2 for baseline. |
| Node 24 final web runtime | No (inferred) | Dispatched | [Supported x64 platform][node-build] and [V8 CPU detection][node-cpu] detect AVX/AVX2 separately. |

Standard floating-point embedding inference uses Torch's CPU dispatch. Specialized
operators, including quantized backends, can have stronger ISA requirements;
this is not a guarantee for every Torch operator or arbitrary downloaded model.
The exact bundled PDFium baseline remains an open point. Image execution on a
Sandy Bridge CPU is still required for end-to-end assurance, including bundled
native libraries and DuckDB extensions. No environment override can repair an
AVX2-only binary.

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
