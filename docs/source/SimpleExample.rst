A Simple Example
================

The above ``=`` for a section seems to be the most appropriate sectioning when using the include directive like in
cookbook.rst.  Or rather don't use a high level sectioning than the title in the rst you include this in.

After using ``=`` we can work our way down the list of sectioning:  ``-`` for subsections and ``^`` for subsubsections.

A Subsection
------------
Notice the tree structure in the left hand panel now.

A subsection within this subsection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Now we are likely over doing it with a subsubsection cause this nestedness is getting messy.


A coding section
----------------
To do an inline code we use tick marks ``import sqlalchemy``.

To do a block of code we do the following:

.. code-block:: python
   :emphasize-lines: 1, 3

    # lets start with some comments
    # Sweet how can emphasize lines
    import sqlalchemy
    engine = sqlalchemy.create_engine("mysql+pymysql://root:password@localhost:8000/server")

Code blocks a easy and they have g reate highlighting.

Here is a list:

* First entry
* second entry why is this like that

   * second sub-entry

* third entry

Let's reference a section in the API documents: :ref:`wpipe.Configuration`.

With the sphinx.ext.autosectionlabel extension specified in conf.py we can simply reference the section
itles that are defined in wpipe.rst and wpipe.sqlintf.rst.