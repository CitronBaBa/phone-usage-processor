

import csv
import os


def saveAsCsv(dir, filename, records):
    outputPath = os.path.join(dir, filename)

    # microst excel only recognize utf-16, change to utf-8 if problematic
    with open(outputPath, 'w', newline='', encoding='utf-16') as file:
        writer = csv.writer(file, dialect='excel',)
        writer.writerows(records)
