from glob import glob
import json
import time

"""
Validate the transformed metadata

"""

def compare(field, oai_val, dpla_val):
    if oai_val != dpla_val:
        print(f"Value for {field} does not match")
        print("DPLA")
        print(dpla_val)
        print("OAI")
        print(oai_val)
        inp = input("Press enter to continue. Press 'c' to continue to next institution.")
        if inp == 'c':
            return False
        return True


def validate_data(file, dpla_data_file, validate_field):
    with open(file, "r") as inf:
        data = json.load(inf)
    inf.close()

    with open(dpla_data_file, "r") as inf:
        dpla_data = json.load(inf)
    inf.close()
    c = True
    for row in data:
        if '.' in validate_field:
            _id = row
            for vfield in validate_field.split('.'):
                _id = _id[vfield]
            _id = _id[0]
        else:
            _id = row[validate_field]
        print(_id)
        if _id not in dpla_data:
            print(f"{_id} not found in data")
            continue
        dpla_row = dpla_data[_id]
        for field, value in row.items():

            if field not in dpla_row:
                print(f"Field {field} not found in DPLA data, but found in OAI data.")
                time.sleep(2)
                continue
            if field == 'sourceResource':
                for _field, _value in value.items():
                    c = compare(_field, _value, dpla_row["sourceResource"][_field])
                    if c == False:
                        return

            else:
                c = compare(field, value, dpla_row[field])
                if c == False:
                    return




def main():
    files = glob("./files/institutions/*.json")
    for file in files:
        print(file.split("/")[-1])
        validate_data(file, "./files/hhub_dpla_ids.json", "@id")
        # validate_data(file, "./files/hhub_dpla_titles.json", "sourceResource.title")




main()

