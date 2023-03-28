import utils
import institutions
import sys
import yaml

institutions_data = institutions.get()

exclude = ['isu']
NAME = 'Automated Crawl Mohub'
RUN_ON = 'workflow_dispatch'

data = {
    'name': NAME,
    'on': RUN_ON,
    'jobs': {}
}

for institution in institutions_data:
    if institution.id in exclude:
        continue
    job_name = f'crawl_{institution.id}'
    job = dict()
    job['env'] = {}
    job['env']['id'] = institution.id
    job['runs-on'] = 'ubuntu-latest'
    job['strategy'] = {
        'matrix': {
            'python-version': ["3.8"]
        }
    }
    job['steps'] = [
        {
            'name': 'checkout',
            'uses': 'actions/checkout@v3',
            'with': {
                'ref': '${{ github.ref }}'
            }
        },
        {
            'name': 'Set up Python ${{ matrix.python-version }}',
            'uses': 'actions/setup-python@v3',
            'with': {
                'python-version': '${{ matrix.python-version }}'
            }
        },
        {
            'name': 'Install dependencies',
            'run': 'python -m pip install --upgrade pip && pip install -r requirements.txt'
        },
        {
            'name': 'Crawl feed',
            'working-directory': './',
            'run': 'python main.py -i $id'
        },
        {
            'name': 'Upload crawl data as artifact',
            'uses': 'actions/upload-artifact@v3',
            'with': {
                'name': '${{ env.id }}_data',
                'path': '${{ env.id}}.json'
            }
        }
    ]

    data['jobs'][job_name] = job

with open(".github/workflows/automated_crawl.yml", "w") as outf:
    outf.write(yaml.dump(data, sort_keys=False).replace("'on'", "on"))