Placeholder so that `go:embed all:tools/windows` compiles before the real helper
binaries are dropped in. Build-time step: place avifenc.exe, exiftool.exe (+ its
exiftool_files/ tree) and ffmpeg.exe (static) under tools/windows/ and they get
baked into the exe. Actual binaries are intentionally NOT committed (*.exe/*.dll
are gitignored); the packaged exe embeds them at build time.

Embedded ffmpeg here is gyan.dev "full_build" (static; libsvtav1 + libaom-av1 +
GPU AV1). ffprobe is not needed and not embedded (the code never calls it).
