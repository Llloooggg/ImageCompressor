Placeholder so that `go:embed all:tools/windows` compiles before the real helper
binaries are dropped in. Build-time step: place avifenc.exe, exiftool.exe (+ its
exiftool_files/ tree) and ffmpeg.exe/ffprobe.exe (static builds) under tools/windows/
and they get baked into the exe. Actual binaries are intentionally NOT committed
(*.exe/*.dll are gitignored); the packaged exe embeds them at build time.

Embedded ffmpeg here is gyan.dev "essentials" (static, includes libaom-av1 + GPU
AV1, but NOT libsvtav1 — the chain degrades to libaom automatically). For SVT-AV1
speed, drop a fuller static build (e.g. BtbN gpl) into tools/ next to the exe:
external tools/ wins over the embedded copy.
