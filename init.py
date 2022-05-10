import os

if not os.path.isdir('files'):
    os.mkdir('files')
    print("Created files directory...")

if not os.path.isdir('files/ingests'):
    os.mkdir('files/ingests')
    print("Created ingests directory...")

if not os.path.isdir('files/institutions'):
    os.mkdir('files/institutions')
    print("Created institutions directory...")

if not os.path.isdir('files/reports'):
    os.mkdir('files/reports')
    print("Created reports directory...")

print("Done.")
