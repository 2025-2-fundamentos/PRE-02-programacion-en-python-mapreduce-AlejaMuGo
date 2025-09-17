"""Taller evaluable"""


# pylint: disable=broad-exception-raised
import fileinput
import glob
import os.path
import time
import re
import os
from itertools import groupby

from toolz.itertoolz import concat, pluck

WORD_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]+")

def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""

    create_directory("files/input")

    raw_dir = os.path.join("files", "raw")
    in_dir = os.path.join("files", "input")

    raw_files = sorted(glob.glob(os.path.join(raw_dir, "*")))
    if not raw_files:
        raise Exception(
            f"No se encontraron archivos en {raw_dir}. "
            "Agrega archivos de texto para poder copiar."
        )

    idx = 0
    for i in range(n):
        for src in raw_files:
            with open(src, "rb") as rf:
                data = rf.read()
            base = os.path.basename(src)
            # nombre único por copia
            dst = os.path.join(in_dir, f"{base}.copy{i:04d}.{idx:06d}.txt")
            with open(dst, "wb") as wf:
                wf.write(data)
            idx += 1


def load_input(input_directory):
    """Funcion load_input"""
    files = glob.glob(os.path.join(input_directory, "*"))
    if not files:
        raise Exception(f"No hay archivos de entrada en {input_directory}")
    return fileinput.input(files=files, openhook=fileinput.hook_encoded("utf-8"))


def preprocess_line(x):
    """Preprocess the line x"""
    if not x:
        return []
    x = x.strip().lower()
    return WORD_RE.findall(x)


def map_line(x):
    """Map: de una línea produce (token, 1) por cada token"""
    tokens = preprocess_line(x)
    for t in tokens:
        yield (t, 1)

def mapper(sequence):
    """Mapper"""
    return concat(map(map_line, sequence))


def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    pairs = list(sequence)
    pairs.sort(key=lambda kv: kv[0])
    return groupby(pairs, key=lambda kv: kv[0])



def compute_sum_by_group(group):
    """Suma los valores de un grupo (k, v) con misma clave"""
    return sum(pluck(1, group))

def reducer(sequence):
    """Reducer"""
    for key, grp in sequence:
        yield key, compute_sum_by_group(grp)


def create_directory(directory):
    """Create Output Directory"""

    if os.path.exists(directory):
        for file in glob.glob(f"{directory}/*"):
            os.remove(file)
        os.rmdir(directory)

    os.makedirs(directory)

def save_output(output_directory, sequence):
    """Save Output"""
    out_path = os.path.join(output_directory, "part-00000")
    items = sorted(sequence, key=lambda kv: kv[0])
    with open(out_path, "w", encoding="utf-8") as f:
        for k, v in items:
            f.write(f"{k}\t{v}\n")


def create_marker(output_directory):
    """Create Marker"""
    success_path = os.path.join(output_directory, "_SUCCESS")
    with open(success_path, "w", encoding="utf-8") as f:
        f.write("")


def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
