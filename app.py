from flask import Flask
from flask import render_template
import json
from collections import OrderedDict

app = Flask(__name__)

@app.route("/")
def index():
    members = get_member_data()
    column_order = ['id', 'institution', 'url', 'metadata_prefix', '@id_prefix', 'include', 'exclude']
    members = [OrderedDict(sorted(member.items(), key=lambda i: column_order.index(i[0]))) for member in members]
    return render_template('index.html', members=members, columns=[key for key, value in members[0].items()])

def get_member_data():
    with open("files/mohub_oai.json", "r") as inf:
        return json.load(inf)

@app.route("/member/<member_id>")
def show_member_info(member_id):
    members = get_member_data()
    member = list(filter(lambda person: person['id'] == member_id, members))[0]
    return render_template('member.html', member=member)

if __name__ == '__main__':
    app.run(debug=True)