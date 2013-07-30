Version information
-------------------

This version of EduSeer is based on revision 556 of the repository at the 
following location:

https://scruffy.ukp.informatik.tu-darmstadt.de/svn/third_party/seersuite/trunk

Removed components wrt to original SeerSuite version
----------------------------------------------------

 - ./src/{perl,bpel]}: The ingestion process should be a separate module.
 - ./webapp/{citeseerx_oaiwebapp,static_site}: The OAI webapp and the static 
    Drupal site are not needed currently and should be split into separate 
    modules.
 - ./crawler, ./resources/heritrix, ./heritrix are only needed for the crawler

