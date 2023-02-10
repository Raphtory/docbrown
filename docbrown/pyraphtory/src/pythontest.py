import pyraphtory as pr


csv_object = pr.PyCSV(path = "/Users/rachelchan/docbrown/docbrown/examples/src/bin/lotr/data/lotr.csv.gz", delimiter= ",", header=False, src_id_column=0, dst_id_column=1,timestamp_column=2)

csv_object.read_csv()