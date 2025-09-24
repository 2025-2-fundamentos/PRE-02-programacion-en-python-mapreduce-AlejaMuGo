"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import glob
import os
import string
import time


def mapreduce(
    mapper,
    reducer,
    input_dir,
    output_dir,
):
    def read_lines_from_files(input_dir):
        sequence = []
        files = glob.glob(f"{input_dir}/*")
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def apply_shuffle_and_sort(pairs_sequence):
        pairs_sequence = sorted(pairs_sequence)
        return pairs_sequence

    def write_results_to_file(result, output_dir):
        with open(f"{output_dir}/part-00000", "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    def create_success_file(output_dir):
        with open(f"{output_dir}/_SUCCESS", "w", encoding="utf-8") as f:
            f.write("")

    def create_output_dir_or_fail(output_dir):
        if os.path.exists(output_dir):
            raise Exception(f"Output directory '{output_dir}' already exists.")
        else:
            os.makedirs(output_dir)

    sequence = read_lines_from_files(input_dir)
    pairs_sequence = mapper(sequence)
    pairs_sequence = apply_shuffle_and_sort(pairs_sequence)
    result = reducer(pairs_sequence)
    create_output_dir_or_fail(output_dir)
    write_results_to_file(result, output_dir)
    create_success_file(output_dir)


def run_experiment(
    n,
    mapper,
    reducer,
    raw_dir,
    input_dir,
    output_dir,
):

    def initialize_directory(directory):
        if os.path.exists(directory):
            for file in glob.glob(f"{directory}/*"):
                os.remove(file)
        else:
            os.makedirs(directory)

    def copy_and_number_raw_files_to_input_folder(raw_dir, input_dir, n=5000):
        for file in glob.glob(f"{raw_dir}/*"):
            with open(file, "r", encoding="utf-8") as f:
                text = f.read()

            for i in range(1, n + 1):
                raw_filename_with_extension = os.path.basename(file)
                raw_filename_without_extension = os.path.splitext(
                    raw_filename_with_extension
                )[0]
                new_filename = f"{raw_filename_without_extension}_{i}.txt"
                with open(f"{input_dir}/{new_filename}", "w", encoding="utf-8") as f2:
                    f2.write(text)

    initialize_directory(input_dir)
    copy_and_number_raw_files_to_input_folder(raw_dir, input_dir, n)

    start_time = time.time()

    mapreduce(
        mapper,
        reducer,
        input_dir,
        output_dir,
    )

    end_time = time.time()

    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")


def wordcount_mapper(sequence):
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend((word, 1) for word in words)

    return pairs_sequence


def wordcount_reducer(pairs_sequence):
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result


if __name__ == "__main__":
    
    run_experiment(
        n=5,  # or any number you want
        mapper=wordcount_mapper,
        reducer=wordcount_reducer,
        raw_dir="files/raw",
        input_dir="files/input",
        output_dir="files/output",
    )

    #

    