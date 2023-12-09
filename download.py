from pathlib import Path
import os
import subprocess

from jsonargparse import ArgumentParser
import pandas as pd

PathLike = str | os.PathLike

def get_downloaded(output_dir: Path) -> set[str]:
    meta_path = output_dir / 'download' / 'metadata.csv'
    if not meta_path.exists():
        return set()
    meta = pd.read_csv(meta_path)
    return set(meta['Series UID'].tolist())

LIST_KEY = 'ListOfSeriesToDownload'

def parse_tcia_file(file_path: PathLike):
    parsed_data = {}
    with open(file_path) as file:
        for line in file.readlines():
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            if '=' in line:
                # It's a key-value pair
                key, value = line.split('=', 1)
                if key == LIST_KEY:
                    parsed_data[key] = []
                else:
                    parsed_data[key] = value
            else:
                parsed_data[LIST_KEY].append(line)

    return parsed_data

def dump_tcia_file(data: dict[str, ...], path: PathLike):
    with open(path, 'w') as f:
        for k, v in data.items():
            match v:
                case list():
                    print(f'{k}=', file=f)
                    for x in v:
                        print(x, file=f)
                case _:
                    print(f'{k}={v}', file=f)

def main():
    parser = ArgumentParser()
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--output_dir', '-o', type=Path, default='.')
    parser.add_argument('--retriever_path', '-r', type=Path, default='/opt/nbia-data-retriever/nbia-data-retriever')
    parser.add_argument('--credential', '-c', type=Path | None, default=None)

    args = parser.parse_args()
    print(args)
    output_dir: Path = args.output_dir.resolve()
    data = parse_tcia_file(args.manifest)
    all_series: list[str] = data[LIST_KEY]
    while True:
        downloaded = set(get_downloaded(output_dir))
        rest = [*filter(lambda x: x not in downloaded, all_series)]
        if len(rest) == 0:
            break
        data[LIST_KEY] = rest
        dump_tcia_file(data, manifest_gen_path := output_dir / 'download.tcia')
        cmd_args = [
            args.retriever_path,
            '-v',
            '-m',
            '-c', manifest_gen_path,
            '-d', output_dir,
        ]
        if args.credential is not None:
            cmd_args += ['-l', args.credential.resolve()]
        print(*cmd_args)
        yemianla = False
        with subprocess.Popen(cmd_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as shell:
            while True:
                if yemianla:
                    line = shell.stderr.readline()
                    if line != '':
                        print(line)
                    elif shell.poll() is not None:
                        break
                else:
                    line = shell.stdout.readline()
                    if line != '':
                        print(line)
                        if "Do you agree with the Data Usage Agreement? (Y/N)" == line.strip():
                            print('ye mian la')
                            yemianla = True
                            print('y', file=shell.stdin, flush=True)

if __name__ == "__main__":
    main()
