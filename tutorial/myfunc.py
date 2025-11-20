from pathlib import Path

dir1 = "/tmp"
dir2 = "/usr"

dir1 = Path(dir1)
dir2 = Path(dir2)
for f in dir2.iterdir():
    if not (dir1 / f.name).exists():
        print(f.name)
