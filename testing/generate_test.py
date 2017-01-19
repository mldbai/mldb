#
# generate_test.py
# Mich, 2016-06-16
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import argparse
import subprocess
import datetime
import os

template = """#
# {filename}
# {author}, {date}
# This file is part of MLDB. Copyright {year} mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class {classname}(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        pass

if __name__ == '__main__':
    mldb.run_tests()"""


def get_filename_from_args():
    parser = argparse.ArgumentParser(description=
        "Create a test file skeleton and create the entry in the makefile.")
    parser.add_argument('filename', help="The filename to create.")
    args = parser.parse_args()
    filename = args.filename

    if not filename.endswith('.py'):
        filename += '.py'

    if os.path.isfile(filename):
        raise Exception("File already exists")

    return filename


def get_class_from_filename(filename):
    capitalize_next = True
    classname = ''

    for c in filename:
        if c == '.':
            break
        if c in ['_', '-']:
            capitalize_next = True
        else:
            if capitalize_next:
                classname += c.capitalize()
                capitalize_next = False
            else:
                classname += c

    return classname


def update_makefile(filename):
    for mk_file, pro in [('testing.mk', False), ('pro_testing.mk', True)]:
        if os.path.isfile(mk_file):
            f = open(mk_file, 'at+')
            break
    else:
        raise Exception("*.mk not found")

    f.seek(-1, 2) # seek post last char
    last_char = f.read()
    if last_char != '\n':
        f.write('\n')
    if pro:
        f.write("$(eval $(call mldb_unit_test,{},pro))"
                .format(filename))
    else:
        f.write("$(eval $(call mldb_unit_test,{}))".format(filename))
    f.close()


def do_it():
    filename = get_filename_from_args()
    author = \
        subprocess.check_output(['git', 'config', '--get', 'user.name'])[:-1]
    now = datetime.datetime.now().isoformat()
    classname = get_class_from_filename(filename)

    update_makefile(filename)

    # create test file
    f = open(filename, 'wt')
    f.write(template.format(filename=filename,
                            author=author,
                            date=now[:10],
                            year=now[:4],
                            classname=classname))
    f.close()

if __name__ == '__main__':
    do_it()
