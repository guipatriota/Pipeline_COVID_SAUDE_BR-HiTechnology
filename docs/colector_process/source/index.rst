.. hiTweets documentation master file, created by
   sphinx-quickstart on Sat Jul 24 13:23:48 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to hiTweets's documentation!
====================================
This document aims to describe the use of hiTweets project to colect, store
into JSON files, process data and input it into a no-SQL data base.


Colector Class
====================================
.. autoclass:: colector.Colector
   :members:


GetException Class
===================================
.. autoclass:: colect_exceptions.GetException
   :members:

ConnectionLostException Class
====================================
.. autoclass:: colect_exceptions.ConnectionLostException
   :members:

Tests
====================================
.. autoclass:: test_colector.TestTwitterCommunication
   :members:

.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
