records\_mover.records package
==============================

Subpackages
-----------

.. toctree::
   :maxdepth: 4

   records_mover.records.sources
   records_mover.records.targets

Submodules
----------

records\_mover.records.base\_records\_format module
---------------------------------------------------

.. autoclass:: records_mover.records.base_records_format.BaseRecordsFormat
   :members:
   :show-inheritance:
   :special-members: __init__

Module contents
---------------

.. comment:

   I exclude PartialRecordsHints entirely below because Sphinx doesn't
   yet support TypedDict types that have hyphens as part of the keys.
   Example error on 'make html':

   WARNING: invalid signature for autoinstanceattribute
   ('records_mover.records::PartialRecordsHints.header-row')

   I exclude RecordsSchema as we haven't defined yet exactly what
   public API to export.

   https://github.com/bluelabsio/records-mover/issues/97

   I exclude RecordsFormat as it's generally an internal factory
   method useful when reading undifferentiated JSON files - a general
   user ought to be able to select their records format in advance and
   use the correct subclass.

   I exclude UntypedRecordsHints as it hasn't been used in any other
   part of the public interface.

.. automodule:: records_mover.records
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: DelimitedRecordsFormat, ParquetRecordsFormat, ProcessingInstructions, PartialRecordsHints, RecordsSchema, RecordsFormat, UntypedRecordsHints

   .. autoclass:: records_mover.records.DelimitedRecordsFormat
      :undoc-members:
      :show-inheritance:
      :special-members: __init__

   .. autoclass:: records_mover.records.ParquetRecordsFormat
      :undoc-members:
      :show-inheritance:
      :special-members: __init__

   .. autoclass:: records_mover.records.ProcessingInstructions
      :undoc-members:
      :show-inheritance:
      :special-members: __init__
