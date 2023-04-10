from glob import glob
import json

def get_data():
    return glob('./*_data/*.json')

def write_report():
    data_files = get_data()
    with open("report.txt", "w") as outf:
        for file in data_files:
            with open(file, 'r') as inf:
                data = json.load(inf)
            skipped = data['skipped']
            count = data['count']
            name = data['institution']
            outf.write(f"# {name}\n")
            outf.write(f"   - {count} records added\n")
            outf.write(f"   - {skipped} records skipped\n\n")
            # for reason, records in data['skipped_errors'].items():
            #     outf.write(f"       - {reason}: {count(records)}\n\n")
            inf.close()

def compile():
    data_files = get_data()
    if len(data_files) < 1:
        raise Exception("No files found to compile!")
    print("Compiling...")
    out = []
    for file in data_files:
        print(file)
        with open(file, "r") as inf:
            data = json.load(inf)
        out.extend(data['records'])
        inf.close()
    outfn = "mohub_ingest.json"
    outfn_l = f"{outfn}l"
    # with open(outfn, "w") as outf:
    #     json.dump(out, outf, indent=4)
    # finish by writing to jsonl, as DPLA prefers
    with open(outfn_l, "w") as outf:
        for line in out:
            json.dump(line, outf)
            outf.write('\n')
    write_report()
    print("Total: {}".format(len(out)))
    print("Wrote ingest file to {}".format(outfn))


compile()

