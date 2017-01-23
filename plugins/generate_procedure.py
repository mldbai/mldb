#
# generate_procedure.py
# Mich, 2016-07-07
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# Create a procedure file skeleton .cc, .h, .html.md and create the entry in
# the makefile.

import argparse
import subprocess
import datetime
import os
from string import capwords


def load_template(name):
    f = open('templates/' + name + '.txt', 'rt')
    return f.read()


def get_proc_from_args():
    parser = argparse.ArgumentParser(description=
        "Create a procedure file skeleton .cc, .h, .html.md and create the "
        "entry in the makefile.")
    parser.add_argument('procedure', help="The procedure to create. "
                                          "Eg.: my.procedure")
    args = parser.parse_args()
    return args.procedure


def update_makefile(filename):
    mk_file = 'plugins.mk'
    if os.path.isfile(mk_file):
        f = open(mk_file, 'rt')
    else:
        raise Exception("*.mk not found")

    makefile = f.read()
    f.close()

    needle = 'LIBMLDB_BUILTIN_PLUGIN_SOURCES:= \\\n'
    idx = makefile.find(needle)
    if idx == -1:
        raise Exception("Failed to find {} in makefile".format(needle))
    new_makefile = '{top}\t{new_file} \\\n{bottom}'.format(
        top=makefile[:idx + len(needle)],
        new_file=filename,
        bottom=makefile[idx + len(needle):])
    f = open(mk_file, 'wt')
    f.write(new_makefile)
    f.close()


def do_it():
    template_cc = load_template('procedure.cc')
    template_h = load_template('procedure.h')
    template_doc = load_template('doc.md')

    proc = get_proc_from_args()
    base_filename = proc.replace('.', '_') + '_procedure'
    author = \
        subprocess.check_output(['git', 'config', '--get', 'user.name'])[:-1]
    now = datetime.datetime.now().isoformat()
    camel_case_proc_name = capwords(proc, '.').replace('.', '')

    update_makefile(base_filename + '.cc')

    f = open(base_filename + '.cc', 'wt')
    f.write(template_cc.format(filename=base_filename,
                               author=author,
                               date=now[:10],
                               year=now[:4],
                               cc_proc_name=camel_case_proc_name))
    f.close()

    f = open(base_filename + '.h', 'wt')
    f.write(template_h.format(filename=base_filename,
                              author=author,
                              date=now[:10],
                              year=now[:4],
                              cc_proc_name=camel_case_proc_name,
                              proc_name=proc))
    f.close()

    f = open('../container_files/public_html/doc/builtin/procedures/'
             + camel_case_proc_name + 'Procedure.md', 'wt')
    f.write(template_doc.format(
        human_proc_title=capwords(proc, '.').replace('.', ' '),
        proc_name=proc))
    f.close()

if __name__ == '__main__':
    do_it()
