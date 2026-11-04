Placeholder so that `go:embed all:tools/windows` compiles before the real helper
binaries are dropped in. Build-time step: place avifenc.exe (+ its DLLs) and
exiftool.exe (+ its exiftool_files/ tree) under tools/windows/ and they get baked
into the exe. Actual binaries are intentionally NOT committed (*.exe/*.dll are
gitignored); the packaged exe embeds them at build time.
