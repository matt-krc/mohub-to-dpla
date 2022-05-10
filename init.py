import os

if not os.path.isdir('files'):
    os.mkdir('files')

if not os.path.isdir('files/ingests'):
    os.mkdir('files/ingests')

if not os.path.isdir('files/institutions'):
    os.mkdir('files/institutions')

if not os.path.isdir('files/reports'):
    os.mkdir('files/reports')
