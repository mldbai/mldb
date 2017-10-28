# MLDB python dependencies

This document describes the mecanisms used by the MLDB build system in order
to keep python dependencies version locked. This is done to control when we
do upgrades of these dependencies and keep the build in a *known good* state.

## python_requirements.txt

[Official doc](https://pip.pypa.io/en/stable/user_guide/#requirements-files)

This file contains all python packages directly required by MLDB.
Dependencies of these packages and their respective versions are listed in
another file named `python_constraints.txt`.

If required by a weird dependency problem, add the problematic dependency
to the requirement file, and *make sure to add a comment* explaining why
this dependency is pulled by the requirements file.

**Note: Versions specified in the requirements file are overriden by those
specified in the constraints file.**

## python_constraints.txt

[Official doc](https://pip.pypa.io/en/stable/user_guide/#constraints-files)

This file contains the list of all python packages that are installed in
the MLDB virtualenv, along with the specific version that must be installed.

Version of packages specified in this file overrides those specified in the
`python_requirements.txt` file.

# Upgrading a single dependency

Start with a fresh virtualenv:

```
rm -rf virtualenv
make python_dependencies

# Activate virtualenv
. virtualenv/bin/activate

# Install all mldb_base deps (required to have all deps in constraints file)
pip install -r python_requirements_mldb_base.txt -c python_constraints.txt
```

Update the dependency:

```

# This will update DEPENDENCY and all its own deps
pip install -U DEPENDENCY
```

Then regenerate the `python_constraints.txt` file:

```
pip freeze -r python_requirements_mldb_base.txt  >python_constraints.txt

# Then edit the top of the contraints file to remove anything before and including
# the '-r python_requirements.txt' line
```

Commit all changes to the python requirements/constraints file.

Once this is done, rebuild all the wheels based on the constraints file:

```
wheelbuild.py -r python_constraints.txt

# upload wheels to wheelhouse as shown in the output of wheelbuild.py
```


# Upgrading all python dependencies

  1. Copy `python_requirements.txt` and remove all version information from
     the copy.

     ```
     cat python_requirements.txt | sed 's/==.*//' > python_requirements.txt-noversion
     ```
     
  2. Create `wheels` for all packages listed in the requirement file using
     the `wheelbuild.py` script.
     This script lives in the `dotfile` repo (`dotfiles/bin/wheelbuild.py`):

     ```
     pip install virtualenv
     wheelbuild.py -r python_requirements.txt-noversion -w ~/tmp/wheelbuild
     ```
  3. The `wheelbuild.py` script outputs the contents of its temporary virtualenv,
     save this output to a temporary file.
  4. Upload the freshly built `wheels` to the wheelhouse,
     using the information output by the script.

  5. Overwrite `python_requirements.txt` using the contents of the temporary
     file saved in step #3.

     - Remove everything under the line:
       ```
       ## The following requirements were added by pip freeze:
       ```
     - Add a line containing the latest available `setuptools` package:
       ```
       setuptools==36.6.0
       ```

  6. Overwrite `python_constraints.txt` using the contents of the temporary
     file saved in step #3.

  7. (Re)move the existing mldb virtualenv:
     ```
     mv virtualenv virtualenv.OLD
     ```
  8. run `make python_dependencies`


