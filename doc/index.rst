.. _gedidb_docs_mainpage:

####################
gediDB documentation
####################

.. toctree::
   :maxdepth: 1
   :hidden:

   User Guide <user/index>
   Installation <user/installing>
   Setting-up database <user/database>
   Development <dev/index>

**Version**: |version|

gediDB is an open source project and Python package that makes working with GEDI L2A-B and L4A-C data easy and fun!


.. grid:: 1 1 2 2
    :gutter: 2 3 4 4

    .. grid-item-card::
        :img-top: /_static/index-images/getting_started.svg
        :text-align: center

        Getting started
        ^^^

        New to GediDB? Check out the quick overview. It contains an
        introduction to GediDB's main functionalitoes.

        +++

        .. button-ref:: user/quick-overview
            :expand:
            :color: secondary
            :click-parent:

            To the quick overview's guide

    .. grid-item-card::
        :img-top: /_static/index-images/user_guide.svg
        :text-align: center

        User guide
        ^^^

        The user guide provides in-depth information on the
        key concepts of GediDB with useful background information and explanation.

        +++

        .. button-ref:: user
            :expand:
            :color: secondary
            :click-parent:

            To the user guide

    .. grid-item-card::
        :img-top: /_static/index-images/database.svg
        :text-align: center

        GEDI database
        ^^^

        The reference guide contains a detailed description on how to setup your own database. 
        It assumes that you have an understanding of the key concepts to build 
        PostGres and PostGIS databases.

        +++

        .. button-ref:: database
            :expand:
            :color: secondary
            :click-parent:

            To the database guide

    .. grid-item-card::
        :img-top: /_static/index-images/contributor.svg
        :text-align: center

        Contributor's guide
        ^^^

        Want to add to the codebase? Can help add translation or a flowchart to the
        documentation? The contributing guidelines will guide you through the
        process of improving gediDB.

        +++

        .. button-ref:: devindex
            :expand:
            :color: secondary
            :click-parent:

            To the contributor's guide

.. This is not really the index page, that is found in
   _templates/indexcontent.html The toctree content here will be added to the
   top of the template header
