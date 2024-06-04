import csv
import struct


def process_csv_to_binary(input_csv, output_bin, column_index):
    with open(input_csv, "r", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter="|")
        data = [
            int(row[column_index]) for row in reader
        ]  # Extract the column and convert to integers

    with open(output_bin, "wb") as binfile:
        # Write the number of items as a 64-bit unsigned integer
        binfile.write(struct.pack("<Q", len(data)))
        # Write each data item as a 64-bit unsigned integer
        for item in data:
            binfile.write(struct.pack("<Q", item))


# Example usage
process_csv_to_binary("sf1/supplier.tbl", "s_suppkey.bin", 0)
