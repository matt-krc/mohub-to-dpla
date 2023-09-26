import utils
import institutions
import sys
import yaml

institutions_data = institutions.get()

exclude = ['isu']
NAME = 'Automated Crawl Heartland Hub'
ON = 'workflow_dispatch'
STRATEGY = {
    'matrix': {
        'python-version': ["3.8"]
    }
}
RUNS_ON = 'ubuntu-latest'
CHECKOUT = {
    'name': 'checkout',
    'uses': 'actions/checkout@v3',
    'with': {
        'ref': '${{ github.ref }}'
    }
}
SET_UP_PYTHON = {
    'name': 'Set up Python ${{ matrix.python-version }}',
    'uses': 'actions/setup-python@v3',
    'with': {
        'python-version': '${{ matrix.python-version }}'
    }
}
INSTALL_DEPENDENCIES = {
    'name': 'Install dependencies',
    'run': 'python -m pip install --upgrade pip && pip install -r requirements.txt'
}

data = {
    'name': NAME,
    'on': ON,
    'jobs': {}
}
crawl_job_names = []
for institution in institutions_data:
    if institution.id in exclude:
        continue
    job_name = f'crawl_{institution.id}'
    crawl_job_names.append(job_name)
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
        CHECKOUT,
        SET_UP_PYTHON,
        INSTALL_DEPENDENCIES,
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

data['jobs']['combine'] = {
    'needs': crawl_job_names,
    'runs-on': RUNS_ON,
    'strategy': STRATEGY,
    'steps': [
        CHECKOUT,
        SET_UP_PYTHON,
        INSTALL_DEPENDENCIES,
        {
            'name': 'Download crawls',
            'uses': 'actions/download-artifact@v3',
            'with': {
                'path': './'
            }
        },
        {
            'name': 'Combine files',
            'working-directory': './',
            'run': 'python combine.py'
        },
        {
            'name': 'Upload jsonl as artifact',
            'uses': 'actions/upload-artifact@v3',
            'with': {
                'name': 'mohub_ingest',
                'path': 'mohub_ingest.jsonl'
            }
        },
        {
            'name': 'Upload ingest report as artifact',
            'uses': 'actions/upload-artifact@v3',
            'with': {
                'name': 'ingest_report',
                'path': 'report.txt'
            }
        }
    ]
}

data['jobs']['upload_s3'] = {
    'runs-on': RUNS_ON,
    'needs': 'combine',
    'strategy': STRATEGY,
    'steps': [
        CHECKOUT,
        SET_UP_PYTHON,
        INSTALL_DEPENDENCIES,
        {
            'name': 'Download jsonl',
            'uses': 'actions/download-artifact@v3',
            'with': {
                'name': 'mohub_ingest'
            }
        },
        {
            'name': 'Upload to S3',
            'env': {
                'AWS_ACCESS_KEY': '${{ secrets.AWS_ACCESS_KEY }}',
                'AWS_SECRET_KEY': '${{ secrets.AWS_SECRET_KEY }}',
                'S3_BUCKET': '${{ secrets.S3_BUCKET }}'
            },
            'working-directory': './',
            'run': 'python upload_s3.py'
        }
    ]
}

yaml.Dumper.ignore_aliases = lambda self, data: True

with open(".github/workflows/automated_crawl.yml", "w") as outf:
    outf.write(yaml.dump(data, Dumper=yaml.Dumper, sort_keys=False).replace("'on'", "on"))