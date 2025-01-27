.. _devindex:

**********************
Contributing to gediDB
**********************

.. highlight:: shell

Overview
========

We welcome your skills and enthusiasm at the gediDB project!. There are numerous opportunities to
contribute beyond just writing code.
All contributions, including bug reports, bug fixes, documentation improvements, enhancement suggestions,
and other ideas are welcome.

This project is a community effort, and everyone is welcome to contribute. Everyone within the community
is expected to abide by our `code of conduct <https://git.gfz-potsdam.de/global-land-monitoring/gedidb/blob/main/CODE_OF_CONDUCT.md>`_.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://git.gfz-potsdam.de/global-land-monitoring/gedidb/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitLab issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitLab issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

gediDB could always use more documentation, whether as part of the
official gediDB docs, in docstrings, or even on the web in blog posts,
articles, and such.  If something in the docs doesn't make sense to you, 
updating the relevant section after you figure it out is a great way to 
ensure it will help the next person.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an `issue <https://git.gfz-potsdam.de/global-land-monitoring/gedidb/issues>`_.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Commit Changes
--------------

How to
~~~~~~

1. Fork the `gedidb` repo on GitLab.
2. Clone your fork locally::

    $ git clone git@git.gfz-potsdam.de:global-land-monitoring/gedidb.git

3. Install your local copy into a virtualenv. Assuming you have virtualenvwrapper installed, this is how you set up your fork for local development::

    $ mkvirtualenv gedidb
    $ cd gedidb/
    $ python setup.py develop

4. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass flake8 and the
   tests, including testing other Python versions with tox::

    $ make pytest
    $ make lint
    $ make urlcheck
    $ tox

   To get flake8 and tox, just pip install them into your virtualenv.

6. Commit your changes and push your branch to GitLab::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

7. Submit a merge request through the GitLab website.

Sign your commits
~~~~~~~~~~~~~~~~~

Please note that our license terms only allow signed commits.
A guideline how to sign your work can be found here: https://git-scm.com/book/en/v2/Git-Tools-Signing-Your-Work

If you are using the PyCharm IDE, the `Commit changes` dialog has an option called `Sign-off commit` to
automatically sign your work.


Merge Request Guidelines
------------------------

Before you submit a pull request, check that it meets these guidelines:

1. The merge request should include tests.
2. If the merge request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for Python 3.6, 3.7, 3.8 and 3.9. Check
   https://git.gfz-potsdam.de/global-land-monitoring/gedidb/-/merge_requests
   and make sure that the tests pass for all supported Python versions.

